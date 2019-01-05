package study.rpc.nio;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

/**
 * 直接缓冲区 与 CopyFile比较(ByteBuffer的allocate:间接缓冲 和allocateDirect:直接缓冲 的区别)
 * 直接缓冲区:  给定一个直接字节缓冲区，Java 虚拟机将尽最大努力直接对它执行本机 I/O 操作。也就是说，它
 * 会在每一次调用底层操作系统的本机 I/O 操作之前(或之后)，尝试避免将缓冲区的内容拷贝到
 * 一个中间缓冲区中(或者从一个中间缓冲区中拷贝数据)
* @Title: FastCopyFile.java
* @author YMY
* @date 2015年6月5日 下午2:59:09 
* @version V1.0
 */
public class FastCopyFile {
	static public void main(String args[]) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: java FastCopyFile infile outfile");
			System.exit(1);
		}

		String infile = args[0];
		String outfile = args[1];

		FileInputStream fin = new FileInputStream(infile);
		FileOutputStream fout = new FileOutputStream(outfile);
		
		//创建通道
		FileChannel fcin = fin.getChannel();
		FileChannel fcout = fout.getChannel();
		
		// 是否同时将文件元数据（权限信息等）写到磁盘上
		fcin.force(true);
		
		ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

		while (true) {
			// buffer清空
			buffer.clear();
			// 从fcin通道中读取内容到buffer
			int r = fcin.read(buffer);
			// 判断读到的内容:如果为-1则没有读到内容,意味着文件读取完毕
			if (r == -1) {
				break;
			}
			// 设置buffer的limit=position和position=0
			buffer.flip();
			// 写出
			fcout.write(buffer);
		}
	}
}
