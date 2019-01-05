package test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * @author seven
 * @since 07.03.2013
 */
public class BulkLoadData2MySQL {

	private static final Logger logger = Logger.getLogger(BulkLoadData2MySQL.class);

	private Connection conn = null;


	public static InputStream getTestDataInputStream() {
		StringBuilder builder = new StringBuilder();
			for (int j = 0; j <= 2; j++) {
				builder.append(4);
				builder.append("\t");
				builder.append(4 + 1);
				builder.append("\n");
			}
		byte[] bytes = builder.toString().getBytes();
		InputStream is = new ByteArrayInputStream(bytes);
		return is;
	}

	/**
	 * 
	 * load bulk data from InputStream to MySQL
	 */
	public int bulkLoadFromInputStream(String loadDataSql,
			InputStream dataStream) throws SQLException {
		if(dataStream==null){
			logger.info("InputStream is null ,No data is imported");
			return 0;
		}
		conn = DriverManager.getConnection("jdbc:mysql://db-server:3306/ymy_test?user=zhaohaijun&password=676892");
		PreparedStatement statement = conn.prepareStatement(loadDataSql);

		int result = 0;

		if (statement.isWrapperFor(com.mysql.jdbc.Statement.class)) {

			com.mysql.jdbc.PreparedStatement mysqlStatement = statement
					.unwrap(com.mysql.jdbc.PreparedStatement.class);

			mysqlStatement.setLocalInfileInputStream(dataStream);
			result = mysqlStatement.executeUpdate();
		}
		return result;
	}

	public static void main(String[] args) {
		String testSql = "LOAD DATA LOCAL INFILE 'sql.csv' IGNORE INTO TABLE ymy_test.scala (id,name)";
		InputStream dataStream = getTestDataInputStream();
		BulkLoadData2MySQL dao = new BulkLoadData2MySQL();
		try {
			
			long beginTime=System.currentTimeMillis();
			int rows=dao.bulkLoadFromInputStream(testSql, dataStream);
			long endTime=System.currentTimeMillis();
			logger.info("importing "+rows+" rows data into mysql and cost "+(endTime-beginTime)+" ms!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.exit(1);
	}

}