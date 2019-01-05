package study.rpc.nio;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 将数据从源通道传输到FileChannel中
* @Title: TransferFrom.java
* @author YMY
* @date 2015年6月5日 下午4:25:41 
* @version V1.0
 */
public class TransferFrom {

	public static void main(String[] args) {
		try {
			// TODO Auto-generated method stub
			RandomAccessFile fromFile = new RandomAccessFile("/opt/test.json", "rw");
			FileChannel fromChannel = fromFile.getChannel();

			RandomAccessFile toFile = new RandomAccessFile("/opt/test.txt", "rw");
			FileChannel toChannel = toFile.getChannel();

			long position = 0;
			long count = fromChannel.size();

			toChannel.transferFrom(fromChannel, position, count);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
