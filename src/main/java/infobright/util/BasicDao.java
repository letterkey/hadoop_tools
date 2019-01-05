package infobright.util;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Stack;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Statement;

public class BasicDao {
	private static final Logger LOG = LoggerFactory.getLogger(BasicDao.class);
	private static Stack<String> stack = new Stack<String>();
	public  static Configuration conf;
	public static <E> boolean excuteLoadData1(InputStream data) {
		Connection c = null;
		Statement stmt = null;
		boolean result = false;
		try {

			LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=");
			String sql = "LOAD DATA LOCAL INFILE 'sql.csv' IGNORE INTO TABLE scala";
			// 创建infobright数据库链接
			c = DriverManager.getConnection(
					"jdbc:mysql://db-server:3306/ymy_test", "zhaohaijun",
					"676892");
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

	public static <E> boolean excuteLoadData(InputStream data) {

		String dbserver = conf.get("dbserver");
		String user = conf.get("username");
		String pwd = conf.get("password");
		String table = conf.get("table");
		
		Connection c = null;
		PreparedStatement p = null;
		int result = 0;
		try {
			LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=\n" + dbserver + "\n" + user + "\n" + pwd + "\n" + table);
			String sql = "LOAD DATA LOCAL INFILE 'src.cvs'  IGNORE INTO TABLE "
					+ table
					+ " fields terminated by '\t'  lines terminated by '\n'";
			// 创建infobright数据库链接
			String jdbc = "jdbc:mysql://" + dbserver + "?user=" + user
					+ "&password=" + pwd;
			LOG.info("--------------------------------------" + jdbc);
			c = DriverManager.getConnection(jdbc);
			p = c.prepareStatement(sql);
			if (p.isWrapperFor(com.mysql.jdbc.Statement.class)) {
				com.mysql.jdbc.PreparedStatement mysqlStatement = p
						.unwrap(com.mysql.jdbc.PreparedStatement.class);
				mysqlStatement.setLocalInfileInputStream(data);
				result = mysqlStatement.executeUpdate();
			}
		} catch (Exception e) {
			LOG.error("basicdao.excuteLoadData", e);
		} finally {
			try {
				if (p != null)
					p.close();
				if (c != null)
					c.close();
			} catch (SQLException e) {
				LOG.error("basicdao.excutequery", e);
			}
		}
		return result > 0;
	}

	/**
	 * 压栈
	 * 
	 * @author YMY
	 * @date 2015年1月28日 下午1:41:53
	 * @param item
	 */
	public static void push(String item) {
		BasicDao.stack.push(item);
		if (!BasicDao.stack.isEmpty() && BasicDao.stack.size() == 2) {
			StringBuffer sb = new StringBuffer();
			String tmp = "";
			int count =0;
			for(int i=1; i <=2; i++){
				if(BasicDao.stack.isEmpty())
					continue;
				tmp = BasicDao.stack.pop();
				if (StringUtils.isNotEmpty(tmp)) {
					tmp.replaceAll("^A","\t");
					sb.append(tmp).append("\n");
				}
			}
			System.out.println("    empty:"+BasicDao.stack.isEmpty()+"  size:"+BasicDao.stack.size()+" data:"+sb.toString());
			BasicDao.excuteLoadData(IOUtils.toInputStream(sb.toString().trim()));
		}
	}

	/**
	 * 压栈
	 * 
	 * @author YMY
	 * @date 2015年1月28日 下午1:41:53
	 * @param item
	 */
	public static void clear() {
		LOG.info("執行basicdao.clear方法isempty:"+BasicDao.stack.isEmpty()+" size:"+BasicDao.stack.size());
		if(BasicDao.stack.isEmpty())
			return;
		StringBuffer sb = new StringBuffer();
		Enumeration<String> items = BasicDao.stack.elements(); // 得到 stack 中的枚举对象
		while (items.hasMoreElements()) { // 显示枚举（stack ） 中的所有元素
			String tmp = items.nextElement();
			if (tmp != "") {
				sb.append(tmp).append("\n");
			}
		}
		System.out.println(sb.toString());
//		BasicDao.excuteLoadData(IOUtils.toInputStream(sb.toString().trim()));
	}

}
