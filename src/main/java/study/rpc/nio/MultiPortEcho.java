package study.rpc.nio;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

/**
 * 异步 I/O
 * 
 * @Title: MultiPortEcho.java
 * @author YMY
 * @date 2015年6月5日 下午3:31:02
 * @version V1.0
 */
public class MultiPortEcho {
	private int ports[];
	private ByteBuffer echoBuffer = ByteBuffer.allocate(1024);

	public MultiPortEcho(int ports[]) throws IOException {
		this.ports = ports;

		go();
	}

	private void go() throws IOException {
		// Create a new selector
		Selector selector = Selector.open();

		// Open a listener on each port, and register each one
		// with the selector
		for (int i = 0; i < ports.length; ++i) {
			ServerSocketChannel ssc = ServerSocketChannel.open();
			// 设置为非阻塞方式
			ssc.configureBlocking(false);
			// 将通道绑定给定的端口
			ServerSocket ss = ssc.socket();
			InetSocketAddress address = new InetSocketAddress(ports[i]);
			ss.bind(address);
			
			// 将通道注册到选择器上并选择accept事件
			//SelectionKey 代表这个通道在此 Selector 上的这个注册
			SelectionKey key = ssc.register(selector, SelectionKey.OP_ACCEPT);
			
			//key.cancel();// 取消注册
			System.out.println("Going to listen on " + ports[i]);
		}

		// 循环选择器
		while (true) {
			int num = selector.select();
			// 获得选择器上的注册器集合
			Set selectedKeys = selector.selectedKeys();
			Iterator it = selectedKeys.iterator();

			while (it.hasNext()) {
				SelectionKey key = (SelectionKey) it.next();
				
				if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
					// Accept the new connection
					ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
					//接受
					SocketChannel sc = ssc.accept();
					sc.configureBlocking(false);

					// Add the new connection to the selector
					SelectionKey newKey = sc.register(selector,SelectionKey.OP_READ);
					it.remove();

					System.out.println("Got connection from " + sc);
				} else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
					// Read the data
					SocketChannel sc = (SocketChannel) key.channel();
					
					// Echo data
					int bytesEchoed = 0;
					while (true) {
						echoBuffer.clear();

						int r = sc.read(echoBuffer);

						if (r <= 0) {
							break;
						}

						echoBuffer.flip();

						sc.write(echoBuffer);
						bytesEchoed += r;
					}
					System.out.println("Echoed " + bytesEchoed + " from " + sc);
					it.remove();
				}
			}
			// System.out.println( "going to clear" );
			// selectedKeys.clear();
			// System.out.println( "cleared" );
		}
	}

	static public void main(String args[]) throws Exception {
		if (args.length <= 0) {
			System.err.println("Usage: java MultiPortEcho port [port port ...]");
			System.exit(1);
		}
		// 监听多个端口
		int ports[] = new int[args.length];

		for (int i = 0; i < args.length; ++i) {
			ports[i] = Integer.parseInt(args[i]);
		}

		new MultiPortEcho(ports);
	}
}
