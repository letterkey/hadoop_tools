package hdfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import com.ymy.util.ImageHelper;

/**
 * HDFS client 
* @Title: HDFSClient.java
* @author YMY
* @date 2015年5月6日 下午3:32:23 
* @version V1.0
 */
public class HDFSClient {
	
	public static void main(String[] args) {
		try{
			File file = new File("/opt/test.jpg");
			InputStream in = new FileInputStream(file);
			HDFSClient hc = new HDFSClient();
//			hc.upload("/test/test.jpg", in);
//			hc.upload("/test/test1.jpg", in,100, 200,500,1000);
			hc.upload("/test/xxxx/yyyy/zoom1.jpg", in,30);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 上传文件到hdfs：源文件
	 *@author YMY 
	 *@date 2015年5月6日 下午4:13:35 
	 *@param path 文件路径和文件名称：/tmp/1/2.txt
	 *@param in	数据输入流
	 *@throws Exception
	 */
	public void upload(String path, InputStream in) throws Exception {
		HDFS.getInstance().uploadFile(path,in);
	}
	
	/**
	 * 上传文件：剪切
	 *@author YMY 
	 *@date 2015年5月6日 下午4:13:35 
	 *@param path 文件路径和文件名称：/tmp/1/2.txt
	 *@param in	数据输入流
	 *@throws Exception
	 */
	public void upload(String path, InputStream in,int x,int y,int w,int h) throws Exception {
		in = ImageHelper.cutImage(in, x,y,w,h);
		HDFS.getInstance().uploadFile(path,in);
	}
	
	/**
	 * 上传文件：图片缩放
	 *@author YMY 
	 *@date 2015年5月7日 上午10:59:16 
	 *@param path
	 *@param in
	 *@param scale 缩放比例 ：50 （50%）
	 *@throws Exception
	 */
	public void upload(String path, InputStream in,int scale) throws Exception {
		in = ImageHelper.zoomImage(in, scale);
		this.upload(path,in);
	}
	
	/**
	 * 下载文件
	 *@author YMY 
	 *@date 2015年5月6日 下午4:13:35 
	 *@param path 文件路径和文件名称：/tmp/1/2.txt
	 *@param in	数据输入流
	 *@throws Exception
	 */
	public void download(String path) throws Exception {
		HDFS.getInstance().downFile();
	}
	
	/**
	 * 删除文件
	 *@author YMY 
	 *@date 2015年5月7日 上午11:08:12 
	 *@param path 文件路径和文件名称：/tmp/1/2.txt
	 *@throws Exception
	 */
	public void delete(String path) throws Exception {
		HDFS.getInstance().deleteFile(path);
	}
}
