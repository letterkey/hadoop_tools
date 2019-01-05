package test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

import com.mysql.jdbc.Statement;

public class LoadDataTest {
	@Test
	public void test_loadData() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://db-server:3306/ymy_test?user=zhaohaijun&password=676892");
			StringBuffer sb = new StringBuffer();
			sb.append("4").append("\t").append("ruzhou").append("\n")
			.append("5").append("\t").append("henan").append("\n");
			
			InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
			
			String sql = "LOAD DATA LOCAL INFILE 'sql1111111111111111.csv' IGNORE INTO TABLE scala";
			stmt = (com.mysql.jdbc.Statement)conn.createStatement();
			stmt.setLocalInfileInputStream(is);
			boolean result = stmt.execute(sql);
		
			System.out.println("Load执行结果：" + result);

		} catch (Exception e) {
			e.printStackTrace();
		}
		// finally {
		// DBUtils.freeConnection();
		// DBUtils.closeQuietly(stmt);
		// DBUtils.closeDataSource();
		// }
	}

	@Test
	public void excuteLoadData() {
		Connection c = null;
		PreparedStatement p = null;
		int result = 0;
		try {

			c = DriverManager.getConnection("jdbc:mysql://db-server:3306/ymy_test?user=zhaohaijun&password=676892");

			String tmp = "4/t中国/n 5/t北京";
			InputStream is = new ByteArrayInputStream(tmp.getBytes());
			String sql = "load data infile '/opt/tmp/test.txt' into table scala fields terminated by '\t'  lines terminated by '\n'";
			p = c.prepareStatement(sql);
			if (p.isWrapperFor(com.mysql.jdbc.Statement.class)) {
				com.mysql.jdbc.PreparedStatement mysqlStatement = p.unwrap(com.mysql.jdbc.PreparedStatement.class);
				mysqlStatement.setLocalInfileInputStream(is);
				result = mysqlStatement.executeUpdate();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (p != null)
					p.close();
				if (c != null)
					c.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	

	@Test
	public void t() {
		StringBuffer sb = new StringBuffer();
		sb.append("xxxxxxxxxxxx").append("\n").append("111111111111").append("\n");
		System.out.println(sb.toString());		System.out.println(sb.toString().trim());
	}
}
