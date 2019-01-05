package test.fs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

/**
 * @author YHT
 * 
 * **/
public class HDFS {
	/***
	 * 加载配置文件
	 * **/
	private Configuration conf;
	private String url;

	@Before
	public void init() {
		conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		// conf.set("", value)
		// conf.addResource("conf/core-site.xml");
		// conf.addResource("conf/hdfs-site.xml");
		url = "hdfs://localhost:9000";
	}

	/***
	 * 读取HDFS某个文件夹的所有 文件，并打印(测试通过)
	 * **/
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
			FileStatus temp[] = hdfs.listStatus(new Path(stats[i].getPath()
					.toString()));
			for (int k = 0; k < temp.length; k++) {
				System.out.println("文件路径名:" + temp[k].getPath().toString()
						+ " 大小：" + temp[k].getLen());
			}
		}
		hdfs.close();
	}

	/**
	 * 重名名一个文件夹或者文件(测试通过)
	 * **/
	@Test
	public void renameFileOrDir() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p1 = new Path(url + "/usr/input/s.txt");
		Path p2 = new Path(url + "/usr/input/s2.txt");
		fs.rename(p1, p2);
		fs.close();// 释放资源
		System.out.println("重命名文件夹或文件成功.....");
	}

	/**
	 * 从HDFS上下载文件或文件夹到本地 (测试通过)
	 * **/
	@Test
	public void downloadFileorDirectoryOnHDFS() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p1 = new Path(url + "/test/input3.txt");
		Path p2 = new Path("D://7");
		fs.copyToLocalFile(p1, p2);
		fs.close();// 释放资源
		System.out.println("下载文件夹或文件成功.....");

	}

	/**
	 * 在HDFS上创建一个文件或文件夹(测试通过)
	 * **/
	@Test
	public void createDir() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(url + "/test/xxx");
		fs.mkdirs(p);
		fs.close();// 释放资源
		System.out.println("创建文件夹成功.....");
	}

	/**
	 * 在HDFS上创建一个文件(测试通过)
	 * 
	 * **/
	@Test
	public void createFileOnHDFS() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(url + "/test/xxx.txt");
		fs.createNewFile(p);
		fs.close();// 释放资源
		System.out.println("创建文件成功.....");
	}

	/**
	 * 在HDFS上删除一个文件夹(测试通过)
	 * **/
	@Test
	public void deleteDirectoryOnHDFS() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(url + "/test/xxx");
		fs.deleteOnExit(p);

		fs.close();// 释放资源
		System.out.println("删除文件夹成功.....");
	}

	/**
	 * 删除文件或者文件夹
	 * 
	 * @param conf
	 * @throws IOException
	 */
	@Test
	public void deleteFile() throws IOException {
		boolean isDeleted = false;
		FileSystem hdfs = FileSystem.get(conf);
		// 删除路径 true=递归删除 false
		Path delPath = new Path("/hello/logs");
		isDeleted = hdfs.delete(delPath, true);
		System.out.println("删除文件夹：" + (isDeleted ? "成功" : "失败"));

		Path delFile = new Path("/hello/hello.txt");
		isDeleted = hdfs.delete(delFile, false);
		System.out.println("删除文件：" + (isDeleted ? "成功" : "失败"));
	}

	/***
	 * 上传本地文件到 HDFS上 测试通过(测试通过)
	 * **/
	@Test
	public void uploadFile() throws Exception {
		// 加载默认配置
		FileSystem fs = FileSystem.get(conf);
		// 本地文件
		Path src = new Path("D:\\log.txt");
		// HDFS为止
		Path dst = new Path(url + "/test/");
		try {
			fs.copyFromLocalFile(src, dst);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("上传成功........");
		fs.close();// 释放资源
	}

	/**
	 * 上传文件到hdfs(读取内容到hdfs)(测试通过)
	 */
	@Test
	public void uploadToHdfs() throws IOException {
		String local = "d://input3.txt";
		String toSrc = url + "/test/input3.txt";
		FileSystem fs = FileSystem.get(conf);

		FileInputStream fis = new FileInputStream(new File(local));
		OutputStream os = fs.create(new Path(toSrc));
		IOUtils.copyBytes(fis, os, 4096, true);

		os.close();
		fis.close();

		System.out.println("上传成功........");
	}

	/**
	 * 以append方式将内容添加到HDFS上文件的末尾;注意：文件更新，需要在hdfs-site.xml中添<property><name>dfs.
	 * append.support</name><value>true</value></property>
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
	 * @throws IOException
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
	 * 写sequencefile序列文件(测试通过)
	 */
	@Test
	public void writeSequenceFile() {
		try {
			String[] data = { "how are you", "hello world",
					"how are you你好 fine thank you", "do you know" };
			String filePath = url + "/test/test.seq";

			FileSystem hdfs = FileSystem.get(conf);
			Path writePath = new Path(filePath);
			IntWritable key = new IntWritable();
			Text value = new Text();
			SequenceFile.Writer writer = SequenceFile.createWriter(hdfs, conf,
					writePath, key.getClass(), value.getClass());
			for (int i = 0; i < data.length; i++) {
				key.set(i);
				value.set(data[i % data.length]);
				writer.append(key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取sequencefile内容(测试通过)
	 * 
	 * @author YMY
	 * @date 2015年2月14日 下午2:19:43
	 * @throws IOException
	 */
	@Test
	public void readSequenceFile() {
		try {
			String src = url + "/test/sequence/log.1430255103466";
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(src), conf);
			LongWritable key = new LongWritable();
			BytesWritable value = new BytesWritable();
			StringBuffer sb = new StringBuffer();
			while (reader.next(key, value)) {
				sb.append(new String(value.getBytes())).append("\n");
			}
			System.out.println(sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 追加序列文件 (测试未通过:sequenceFile不可以append操作)
	 * @author YMY
	 * @date 2015年2月14日 下午2:22:33
	 */
	@Test
	public void appendSeqHdfsFile() {
		try {
			conf.set("dfs.support.append", "true");
			String[] data = { "hello world", "yes i do", "do you know",
					"11111111111111111111111111", "i like you too" };
			String filePath = url + "/test/test.seq";

			FileSystem hdfs = FileSystem.get(URI.create(filePath), conf);
			Path writePath = new Path(filePath);
			IntWritable key = new IntWritable();
			Text value = new Text();

			SequenceFile.Writer writer = SequenceFile.createWriter(hdfs, conf,
					writePath, key.getClass(), value.getClass());
			for (int i = 0; i < data.length; i++) {
				key.set(i);
				value.set(data[i % data.length]);
				writer.append(key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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
			String dst = url + "/tmp/x.txt";
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream hdfsInStream = fs.open(new Path(dst));
			OutputStream out = new FileOutputStream("/tmp/xxx.txt");
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
	}

	/**
	 * 查看文件的所有块信息 (测试通过)
	 *
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

	/**
	 * 读取fsimage镜像文件  （测试未通过）
	 *
	 * @author YMY
	 * @date 2015年2月14日 下午1:04:30
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void readFsImage() throws FileNotFoundException, IOException {
		DataInputStream in = new DataInputStream(new BufferedInputStream(
				new FileInputStream("d:\\fsimage")));

		// read image version: first appeared in version -1
		int imgVersion = in.readInt();
		// read namespaceID: first appeared in version -2
		int namespaceID = in.readInt();

		// read number of files
		long numFiles = in.readLong();

		// read in the last generation stamp.
		long genstamp = in.readLong();

		short namelength = in.readShort();
		byte[] data = new byte[namelength];
		in.readFully(data, 0, namelength);

		String path = new String(data);
		short replication = in.readShort();
		long modificationTime = in.readLong();
		long atime = in.readLong();
		long blockSize = in.readLong();
		int numBlocks = in.readInt(); // 实际值也是-1

		long nsQuota = in.readLong();
		long dsQuota = in.readLong();
	}

	/**
	 * 复制文件 (测试通过)
	 * 
	 * @param conf
	 * @throws IOException
	 */
	@Test
	public void copyFile() throws IOException {
		FileSystem fs = FileSystem.get(conf);

		// 本地文件
		Path src = new Path("D:\\logs");
		// HDFS为止
		Path dst = new Path("/hello/logs");
		fs.copyFromLocalFile(src, dst);
	}

	/**
	 * 读取路径下所有文件 (测试通过)
	 * 
	 * @param conf
	 * @throws IOException
	 */
	@Test
	public void findFile() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path listf = new Path("/");
		FileStatus stats[] = fs.listStatus(listf);
		for (int i = 0; i < stats.length; ++i) {
			System.out.println(stats[i].getPath().toString());
		}
		fs.close();
	}

	/**
	 * 文件合并  （测试未通过）
	 *@author YMY 
	 *@date 2015年4月10日 下午4:15:50
	 */
	@Test
	public void concat() {
		try {
			String url = "hdfs://localhost:9000/";
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(url), conf);
			Path to = new Path(url + "/out/test.txt");
			Path from = new Path(url + "/2014/12/26/mobile/13/NOTICE.txt");
			Path from2 = new Path(
					url + "/2014/12/26/mobile/13/yarn-root-resourcemanager-localhost.log");
			Path[] src = { from };
			fs.concat(to, src);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 合并文件到新文件  （测试未通过）
	 */
	@Test
	public void copyMerge() {
		try {
			String url = "hdfs://localhost:9000/";
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(url), conf);
			Path to = new Path(url + "/tmp/1/" + "x.txt");
			Path from = new Path(url + "/2014/12/26/tpm/13/");

			FileUtil fu = new FileUtil();
			fu.copyMerge(fs, from, fs, to, false, conf, "测试 copemerge");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
