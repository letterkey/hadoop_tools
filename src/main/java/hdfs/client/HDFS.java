package hdfs.client;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import hdfs.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


/**
 * hdfs基础api测试
 * 
 * @Title: TestHDFS.java
 * @author YMY
 * @date 2015年2月14日 下午4:55:34
 * @version V1.0
 */
public class HDFS {
	private static HDFS instance = new HDFS();
	private static Configuration conf;
	private static String url;

	private HDFS() {
		conf = new Configuration();
		url = PropertiesUtil.getInstance().getString("hdfs");
	}

	public static HDFS getInstance() {
		return instance;
	}

	public static void main(String[] args) {
		try {

			HDFS.getInstance().readFromHdfs();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 从HDFS上下载文件或文件夹到本地 (测试通过)
	 * @author YMY
	 * @date 2015年4月10日 下午5:47:45
	 * @throws Exception
	 */
	public void downFile() throws Exception {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path input = new Path(url + "/tmp/x.txt");
			Path out = new Path("/tmp/");
			fs.copyToLocalFile(input, out);
			fs.close();
			System.out.println("下载文件夹或文件成功.....");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 上传文件
	 *@author YMY 
	 *@date 2015年5月7日 上午11:26:27 
	 *@param toFile
	 *@param in
	 *@throws Exception
	 */
	public void uploadFile(String toFile, InputStream in) throws Exception {
		String dst = url + toFile;
		Path filePath = new Path(dst);
		FileSystem fs = FileSystem.get(conf);
		fs.createNewFile(filePath);
		
		FSDataOutputStream out = fs.create(filePath);
		
		byte[] tmp = new byte[1024];
		// 读入多个字节到字节数组中，byteread为一次读入的字节数
		while (in.read(tmp) != -1) {
			out.write(tmp);
		}
		out.close();
		fs.close();

	}


	/**
	 * 读取文件内容(测试通过)
	 * 
	 * @author YMY
	 * @date 2015年2月14日 下午2:14:51
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void readFromHdfs() throws FileNotFoundException, IOException {
		try {
			String dst = url + "/tmp/tmp.jar";
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream hdfsInStream = fs.open(new Path(dst));
			
			OutputStream out = new FileOutputStream("/tmp/1.jar");
			byte[] ioBuffer = new byte[1024];
			int readLen = hdfsInStream.read(ioBuffer);
			while (-1 != readLen) {
				out.write(ioBuffer, 0, readLen);
				readLen = hdfsInStream.read(ioBuffer);
			}
			out.close();
			hdfsInStream.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("写出成功........");
	}

	/**
	 * 在HDFS上创建一个文件或文件夹(测试通过)
	 *
	 * @author YMY
	 * @date 2015年4月10日 下午5:42:40
	 * @throws Exception
	 */
	@Test
	public void createDir() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(url + "/tmp/2/3/4");
		fs.mkdirs(p);
		fs.close();// 释放资源
		System.out.println("创建文件夹成功.....");
	}

	/**
	 * 在HDFS上创建一个文件(测试通过)
	 *
	 * @author YMY
	 * @date 2015年4月10日 下午5:43:01
	 * @throws Exception
	 */
	@Test
	public void createFileOnHDFS() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(url + "/tmp/2/xxx.txt");
		fs.createNewFile(p);
		fs.close();// 释放资源
		System.out.println("创建文件成功.....");
	}

	/**
	 * 在HDFS上删除一个文件夹(测试通过)
	 * @author YMY
	 * @date 2015年4月10日 下午5:43:18
	 * @throws Exception
	 */
	@Test
	public void deleteDirectoryOnHDFS() {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path p = new Path(url + "/tmp/2");
			fs.deleteOnExit(p);
			fs.close();
			System.out.println("删除文件夹成功.....");
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除文件
	 *@author YMY 
	 *@date 2015年5月7日 上午10:21:11 
	 *@param filePath
	 *@return
	 *@throws Exception
	 */
	public boolean deleteFile(String filePath) throws Exception {
		boolean result = true;
			FileSystem hdfs = FileSystem.get(conf);
			Path file = new Path(filePath);
			FileStatus status = hdfs.getFileStatus(file);
			// 如果是文件，则删除
			if(status.isFile()){
				result = hdfs.delete(file, true);
			}
			return result;
	}

	/**
	 * 以append方式将内容添加到HDFS上文件的末尾
	 *
	 * @author YMY
	 * @date 2015年4月10日 下午5:48:59
	 */
	@Test
	public void appendToHdfs() {
		try {
			conf.set("dfs.support.append", "true");
			String dst = url + "/tmp/out/part-r-000000";
			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			FSDataOutputStream out = fs.append(new Path(dst));
			String context = "zhangzk add by hdfs java api";
			int readLen = context.getBytes().length;
			while (-1 != readLen) {
				out.write(context.getBytes(), 0, readLen);
			}
			out.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 重命名文件 (测试通过)
	 *
	 * @author YMY
	 * @date 2015年4月10日 下午5:49:16
	 */
	@Test
	public void renameFile() {
		try {
			FileSystem hdfs = FileSystem.get(conf);
			Path frpaht = new Path("/hello2"); // 旧的文件名
			Path topath = new Path("/test"); // 新的文件名
			boolean isRename = hdfs.rename(frpaht, topath);
			System.out.println("文件重命名结果为：" + (isRename ? "成功" : "失败"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
