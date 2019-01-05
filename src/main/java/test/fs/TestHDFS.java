package test.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 *  hdfs基础api测试
 * @Title: TestHDFS.java
 * @author YMY
 * @date 2015年2月14日 下午4:55:34 
 * @version V1.0
 */
public class TestHDFS {
	private Configuration conf;
	private String url;
	@Before
	public void init() {
		conf = new Configuration();
		url = "hdfs://localhost:9000";
	}

	/**
	 * 读取HDFS某个文件夹的所有文件
	 *@author YMY 
	 *@date 2015年2月14日 下午4:59:51 
	 *@throws Exception
	 */
	@Test
	public void readHDFSListAll() throws Exception {
		FileSystem hdfs = FileSystem.get(conf);
		// 使用缓冲流，进行按行读取的功能
		BufferedReader buff = null;
		// 获取日志文件的根目录
		Path listf = new Path(url + "/test");
		// 获取根目录下的所有2级子文件目录
		FileStatus stats[] = hdfs.listStatus(listf);
		// 自定义j，方便查看插入信息
		int j = 0;
		for (int i = 0; i < stats.length; i++) {
			// 获取子目录下的文件路径
			FileStatus temp[] = hdfs.listStatus(new Path(stats[i].getPath().toString()));
			for (int k = 0; k < temp.length; k++) {
				System.out.println("文件路径:" + temp[k].getPath().toString()+ " 大小：" + temp[k].getLen());
			}
		}
		hdfs.close();
	}

	/**
	 * 重名名一个文件夹或者文件(测试通过)
	 *@author YMY 
	 *@date 2015年2月14日 下午5:01:33 
	 *@throws Exception
	 */
	@Test
	public void renameFileOrDir() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path in = new Path(url + "/usr/input/s.txt");
		Path out = new Path(url + "/usr/input/s2.txt");
		fs.rename(in, out);
		fs.close();
		System.out.println("重命名文件夹或文件成功.....");
	}

	/**
	 * 从HDFS上下载文件或文件夹到本地 (测试通过)
	 * 
	 *@author YMY 
	 *@date 2015年4月10日 下午5:47:45 
	 *@throws Exception
	 */
	@Test
	public void downFile() throws Exception {
		try{
			FileSystem fs = FileSystem.get(conf);
			Path input = new Path(url + "/tmp/x.txt");
			Path out = new Path("/tmp/");
			fs.copyToLocalFile(input, out);
			fs.close();
			System.out.println("下载文件夹或文件成功.....");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 上传本地文件到 HDFS上 测试通过(测试通过)
	 *@author YMY 
	 *@date 2015年4月10日 下午5:47:59 
	 *@throws Exception
	 */
	@Test
	public void uploadFile() throws Exception {
		// 加载默认配置
		FileSystem fs = FileSystem.get(conf);
		// 本地文件
		Path input = new Path("/usr/local/filecrush.jar");
		// HDFS为止
		Path out = new Path(url + "/tmp/");
		try {
			fs.copyFromLocalFile(input, out);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("上传成功........");
		fs.close();
	}
	
	/**
	 * 写文件到hdfs(测试通过)
	 *@author YMY 
	 *@date 2015年4月10日 下午5:48:14 
	 *@throws IOException
	 */
	@Test
	public void writeToHdfs() throws IOException {
		String local = "/usr/local/filecrush.jar";
		String toSrc = url + "/tmp/tmp.jar";
		FileSystem fs = FileSystem.get(conf);
		FileInputStream fis = new FileInputStream(new File(local));
		OutputStream os = fs.create(new Path(toSrc));
		IOUtils.copyBytes(fis, os, 4096, true);
		os.close();
		fis.close();
		System.out.println("写入成功........");
	}

	/**
	 * 读取文件内容(测试通过)
	 * @author YMY
	 * @date 2015年2月14日 下午2:14:51
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void readFromHdfs() throws FileNotFoundException, IOException {
		try{
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
		}catch(Exception e){
			e.printStackTrace();
		}
		System.out.println("写出成功........");
	}
	
	/**
	 * 在HDFS上创建一个文件或文件夹(测试通过)
	 *@author YMY 
	 *@date 2015年4月10日 下午5:42:40 
	 *@throws Exception
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
	 *@author YMY 
	 *@date 2015年4月10日 下午5:43:01 
	 *@throws Exception
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
	 *@author YMY 
	 *@date 2015年4月10日 下午5:43:18 
	 *@throws Exception
	 */
	@Test
	public void deleteDirectoryOnHDFS(){
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
	 * 删除文件或者文件夹
	 *@author YMY 
	 *@date 2015年4月10日 下午5:48:38
	 */
	@Test
	public void deleteFile(){
		try {
			boolean isDeleted = false;
			FileSystem hdfs = FileSystem.get(conf);
			// 删除路径 true=递归删除
			Path delPath = new Path("/hello/logs");
			isDeleted = hdfs.delete(delPath, true);
			System.out.println("删除文件夹：" + (isDeleted ? "成功" : "失败"));
			Path delFile = new Path("/hello/hello.txt");
			isDeleted = hdfs.delete(delFile, false);
			System.out.println("删除文件：" + (isDeleted ? "成功" : "失败"));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 以append方式将内容添加到HDFS上文件的末尾
	 *@author YMY 
	 *@date 2015年4月10日 下午5:48:59
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
	 *@author YMY 
	 *@date 2015年4月10日 下午5:49:16
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

	/**
	 * 查看文件的所有块信息 (测试通过)
	 * @author YMY
	 * @date 2015年2月14日 下午2:14:23
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void getBlocksLocations() throws FileNotFoundException, IOException {
		String src = url + "/tmp/x.txt";
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(src);
		FileStatus fileStatus = hdfs.getFileStatus(path);
		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus,
				0, fileStatus.getLen());

		for (BlockLocation bl : blkLocations) {
			String tmp = "";
			for (String host : bl.getHosts()) {
				tmp += host + " ";
			}

			String names = "";
			for (String name : bl.getNames())
				names += name + " ";
			String paths = "";
			for (String p : bl.getTopologyPaths())
				paths += p + " ";
			System.out.println(names + "\n" + tmp + "\n" + paths);
		}
	}
	
	/**
	 * 获得集群中datanode节点 (测试通过)
	 *@author YMY 
	 *@date 2015年4月10日 下午5:49:33 
	 *@throws FileNotFoundException
	 *@throws IOException
	 */
	@Test
	public void getDataNodes() throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		System.out.println(hdfs.getCanonicalServiceName());
		for (int i = 0; i < dataNodeStats.length; i++) {
			String node = dataNodeStats[i].getHostName();
			System.out.println(node);
		}
	}
}
