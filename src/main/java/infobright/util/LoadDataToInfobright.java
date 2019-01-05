package infobright.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Stack;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Statement;

public class LoadDataToInfobright {
	private static final Logger LOG = LoggerFactory
			.getLogger(LoadDataToInfobright.class);
	private static Stack<String> stack = new Stack<String>();
	public static Configuration conf;

	public static <E> boolean excuteLoadData1(InputStream data) {
		Connection c = null;
		Statement stmt = null;
		boolean result = false;
		try {
			String sql = "LOAD DATA LOCAL INFILE 'sql.csv' IGNORE INTO TABLE machinemeasurement_test";
			// 创建infobright数据库链接
//			c = DriverManager.getConnection(
//					"jdbc:mysql://db-server:3306/ymy_test", "zhaohaijun",
//					"676892");
			c = DriverManager.getConnection(
			"jdbc:mysql://10.251.73.171:5029/mobileprivatedata", "mobilesaas",
			"IZqniH8TNnTt10O");
			stmt = (com.mysql.jdbc.Statement) c.createStatement();
			stmt.setLocalInfileInputStream(data);
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("basicdao.excuteLoadData", e);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (c != null)
					c.close();
			} catch (SQLException e) {
				LOG.error("basicdao.excutequery", e);
			}
		}
		return result;
	}

	public static void main(String[] args) {
		try {
			StringBuffer sb = new StringBuffer();
			String pathname = args[0]; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径
			File filename = new File(pathname); // 要读取以上路径的input。txt文件
			InputStreamReader reader = new InputStreamReader(
					new FileInputStream(filename)); // 建立一个输入流对象reader
			BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
			String line = "";
			line = br.readLine();
			int total = 0;
			while (line != null) {
				line = br.readLine(); // 一次读入一行数据
				sb.append(line).append("\n");
				total++;
				if (total >= 30000) {
					BasicDao.excuteLoadData(IOUtils.toInputStream(sb.toString()
							.trim()));
					total=0;
					sb = new StringBuffer();
				}
			}
			LoadDataToInfobright.excuteLoadData1(IOUtils.toInputStream(sb.toString().trim()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
