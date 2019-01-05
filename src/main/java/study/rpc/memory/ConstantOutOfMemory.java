package study.rpc.memory;

import java.util.ArrayList;
import java.util.List;

/**
 * 常量池内存溢出探究
 * 
 * @Title: ConstantOutOfMemory.java
 * @author YMY
 * @date 2015年6月4日 下午5:48:50
 * @version V1.0
 */
public class ConstantOutOfMemory {
	/**
	 * 
	 * @param args
	 * 
	 * @throws Exception
	 * 
	 * @Author YHJ create at 2011-10-30 下午04:28:25
	 */
	public static void main(String[] args) throws Exception {
		try {

			List<String> strings = new ArrayList<String>();

			int i = 0;

			while (true) {

				strings.add(String.valueOf(i++).intern());

			}

		} catch (Exception e) {

			e.printStackTrace();

			throw e;

		}

	}
}
