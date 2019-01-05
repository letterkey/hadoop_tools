package study.rpc.nio;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

/**
 * 内存映射文件 I/O
* @Title: UseMappedFile.java
* @author YMY
* @date 2015年6月5日 下午3:22:30 
* @version V1.0
 */
public class UseMappedFile {
	static private final int start = 0;
	static private final int size = 1024;

	static public void main(String args[]) throws Exception {
		RandomAccessFile raf = new RandomAccessFile("/opt/test.json", "rw");
		FileChannel fc = raf.getChannel();

		MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, start, size);

		mbb.put(0, (byte) 97);
		mbb.put(1023, (byte) 122);
		for(int i=0; i <mbb.capacity();++i){
			System.out.println(mbb.get());
		}
		raf.close();
	}
}

