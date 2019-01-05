package test.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Pretest {

	public static void main(String args[]) throws SQLException,
			ClassNotFoundException {
		String jdbcdriver = "org.apache.hive.jdbc.HiveDriver";
		String jdbcurl = "jdbc:hive2://10.128.17.21:10000";

		String username = "hadoop";
		String password = "";
		Class.forName(jdbcdriver);
		Connection c = DriverManager.getConnection(jdbcurl, username, password);
		Statement st = c.createStatement();

		print("num should be 1 ", st.executeQuery("select * from tpm.authorization_test limit 1"));
		// ( "select id,name,vip from users order by id limit 5" ) );
		// TODO indexing
	}

	static void print(String name, ResultSet res) throws SQLException {
		System.out.println(name);
		ResultSetMetaData meta = res.getMetaData();
		// System.out.println( "\t"+res.getRow()+"条记录");
		String str = "";
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			str += meta.getColumnName(i) + "   ";
			// System.out.println( meta.getColumnName(i)+"   ");
		}
		System.out.println("\t" + str);
		str = "";
		while (res.next()) {
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				str += res.getString(i) + "   ";
			}
			System.out.println("\t" + str);
			str = "";
		}
	}
}
