package hdfs.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 加载属性文件
* @Title: PropertiesUtil.java
* @author YMY
* @date 2015年5月6日 下午3:20:44 
* @version V1.0
 */
public final class PropertiesUtil {
	private static Properties prop = new Properties();
	private static PropertiesUtil instance = new PropertiesUtil();
	public static PropertiesUtil getInstance(){
		return instance;
	}

	private PropertiesUtil() {
		InputStream in = PropertiesUtil.class.getResourceAsStream("/hdfs.properties");
		try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getString(String key) {
		return (String)prop.get(key);
	}

	public static int getInt(String key) {
		return Integer.valueOf(getString(key).trim());
	}

	public static void main(String args[]) {
		String port = PropertiesUtil.getInstance().getString("hdfs");
		System.out.println(port);
	}
}