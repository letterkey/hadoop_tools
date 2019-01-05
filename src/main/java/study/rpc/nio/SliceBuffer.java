package study.rpc.nio;


import java.nio.ByteBuffer;

/**
 * 缓冲区分片 bytebuffer的分片
* @Title: SliceBuffer.java
* @author YMY
* @date 2015年6月5日 下午3:06:31 
* @version V1.0
 */
public class SliceBuffer {
	static public void main(String args[]) throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(10);

		for (int i = 0; i < buffer.capacity(); ++i) {
			buffer.put((byte) i);
		}
		
		buffer.position(3);
		buffer.limit(7);
		// 分片
		ByteBuffer slice = buffer.slice();
		// 对分片buffer修改元素值
		for (int i = 0; i < slice.capacity(); ++i) {
			byte b = slice.get(i);
			b *= 11;
			slice.put(i, b);
		}
		
		buffer.position(0);
		buffer.limit(buffer.capacity());
		// 验证源buffer
		while (buffer.remaining() > 0) {
			System.out.println(buffer.get());
		}
	}
}
