package study.rpc.nio;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 数据从FileChannel传输到其他的channel
* @Title: TransferTo.java
* @author YMY
* @date 2015年6月5日 下午4:25:13 
* @version V1.0
 */
public class TransferTo {

	public static void main(String[] args) {
		try {
			RandomAccessFile fromFile = new RandomAccessFile("fromFile.txt",	"rw");
			FileChannel fromChannel = fromFile.getChannel();

			RandomAccessFile toFile = new RandomAccessFile("toFile.txt", "rw");
			FileChannel toChannel = toFile.getChannel();

			long position = 0;
			long count = fromChannel.size();

			fromChannel.transferTo(position, count, toChannel);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
