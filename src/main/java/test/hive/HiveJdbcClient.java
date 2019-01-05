package test.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection(
				"jdbc:hive://10.128.17.21:10000", "hadoop",
				"000000");
		Statement stmt = con.createStatement();
		ResultSet res = stmt.executeQuery("select  cookie_id ,COUNT(cookie_id)count  from cookie_info where uid=5779665 group by cookie_id ORDER BY count DESC limit 20");
		while (res.next()) {
			System.out.println(res.getString(1) + "         " + res.getInt(2));
		}
	}
}