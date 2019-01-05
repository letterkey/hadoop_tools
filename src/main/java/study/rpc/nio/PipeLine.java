package study.rpc.nio;

import java.nio.ByteBuffer;
import java.nio.channels.Pipe;

/**
 * 管道是2个线程之间的单向数据连接。Pipe有一个source通道和一个sink通道。数据会被写到sink通道，从source通道读取
* @Title: PipeLine.java
* @author YMY
* @date 2015年6月5日 下午4:36:25 
* @version V1.0
 */
public class PipeLine {
	public static void main(String[] args) {
		try {
			Pipe pipe = Pipe.open();

			Pipe.SinkChannel sinkChannel = pipe.sink();

			String newData = "New String to write to file..." + System.currentTimeMillis();
			ByteBuffer buf = ByteBuffer.allocate(48);
			buf.clear();
			buf.put(newData.getBytes());
			buf.flip();
			while (buf.hasRemaining()) {
				sinkChannel.write(buf);
			}

			
			Pipe.SourceChannel sourceChannel = pipe.source();
			buf.clear();
			 
			int bytesRead = sourceChannel.read(buf);
			System.out.println(bytesRead);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
