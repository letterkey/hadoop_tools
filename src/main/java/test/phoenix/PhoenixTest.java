package test.phoenix;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class PhoenixTest {
	
	@Test
	public void batchInsert(){
		try {
			String table = "DUAL";
			Statement stmt = null;
			ResultSet rset = null;
			Properties props = new Properties();
//			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(System.currentTimeMillis()-1000*60*60*24*2));

//			Connection con = DriverManager.getConnection("jdbc:phoenix:localhost",props);
			Connection con = DriverManager.getConnection("jdbc:phoenix:10.128.17.144:2181:/hbase_test",props);
			
			stmt = con.createStatement();
			String create = "create table " + table + " (mykey integer not null primary key, mycolumn varchar)";
			String  metric_256="create table  metric_data_entity_256(salt integer not null, data_version smallint not null, application_id integer not null, time_scope integer not null , metric_type_id bigint not null , metric_id bigint not null , time  smallint not null , agent_run_id integer not null, uuid integer not null, num1 float, num2 float, num3 float, num4 float, num5 float, num6 float CONSTRAINT  pk PRIMARY KEY (salt,data_version,application_id,time_scope desc,metric_type_id,metric_id,time desc,agent_run_id,uuid) )";
//			stmt.execute(create);
//			stmt.execute(metric_256);
//			con.commit();

//			stmt.executeUpdate("upsert into  "+ table+"(mykey,mycolumn) values(15,'13hhhhhhhhhhhhhhhhhh')");
			stmt.executeUpdate("upsert into metric_data_entity_256(salt,data_version,application_id,time_scope,metric_type_id,metric_id,time,agent_run_id,uuid,num1,num2,num3,num4,num5,num6)values(18,15,12,6,3,16, 4352, 1, 14, 0, 0 ,1339632,1,2,3)");
			con.commit();
//			stmt.executeUpdate("upsert into " + table + " values (14,'Hello--------------------')");
//			stmt.addBatch("upsert into  "+ table+"(mykey,mycolumn) values(10,'hhhhhhhhhhhhhhhhhh')");
//			stmt.addBatch("upsert into  "+ table+"(mykey,mycolumn) values(12,'hhhhhhhhhhhhhhhhhh')");
//			stmt.addBatch("upsert into  "+ table+"(mykey,mycolumn) values(14,'12312hhhhhhhhhhhhhhhhhh')");
//			stmt.executeBatch();
//			con.commit();
//			int k =0 ;
//			for (int i=0; i < 100000000;i ++){
//				k = i;
//				stmt.executeUpdate("upsert into  "+ table+"(id,n1) values("+i+",'uuuuuuuuuuu44444444444444444444444444444uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu')");
//				con.commit();
//			}
//			
//			System.out.println(k);
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

	/**
	 * 根据sql查询结果列表
	 *
	 * @author YMY
	 * @date 2015年6月10日 下午2:39:34
	 * @param sql
	 * @return
	 */
	public static List<String> query(String sql) {
		System.out.println(sql+"-----------------------------------------");
		List<String> data = new ArrayList<String>();
		Connection con = null;
		PreparedStatement statement = null;
		ResultSet rset = null;

		try {
			Properties props = new Properties();
			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
					Long.toString(System.currentTimeMillis() - 1000 * 60 * 60
							* 24 * 2));

			con = DriverManager.getConnection("jdbc:phoenix:10.128.17.20:2181:/hbase_test", props);

			statement = con.prepareStatement("select * from test5 where 1=1 limit 1 ");
			rset = statement.executeQuery();

			int num = rset.getMetaData().getColumnCount();

			while (rset.next()) {
				String row = "";
				for (int i = 1; i <= num; i++) {
					row = row + rset.getObject(i) + "\t";
				}
				System.out.println(row);
				data.add(row.trim());
			}
		} catch (Exception e) {
			e.printStackTrace();
			ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
			e.printStackTrace(new java.io.PrintWriter(buf, true));
			String expMessage = buf.toString();
			buf.close();

//			data.add(expMessage);
		} finally {
			if (rset != null) {
				try {
					rset.close();

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return data;
		}
	}
	
	public static void xxx(String[] args) {
		PhoenixTest.query("");
	}
	public static void main(String[] args) {
		try {
			System.out.println("------------------------------"+args.length);
			String table = "metric_data_entity_256";
			Statement stmt = null;
			ResultSet rset = null;
			
			Connection con = DriverManager.getConnection("jdbc:phoenix:10.128.17.16:2181:/hbase_test");
//			stmt = con.createStatement();
//			String create = "create table " + table + " (mykey integer not null primary key, mycolumn varchar)";
////			System.out.println(create);
////			stmt.executeUpdate(create);
//			stmt.executeUpdate("upsert into " + table + " values (14,'Hello--------------------')");
//			con.commit();
//			stmt.addBatch("upsert into  "+ table+"(mykey,mycolumn) values(10,'hhhhhhhhhhhhhhhhhh')");
//			stmt.addBatch("upsert into  "+ table+"(mykey,mycolumn) values(12,'hhhhhhhhhhhhhhhhhh')");
//			stmt.addBatch("upsert into  "+ table+"(mykey,mycolumn) values(13,'hhhhhhhhhhhhhhhhhh')");
//			stmt.executeBatch();
//			con.commit();
			
			// 批量
//			PreparedStatement batchInsert = con.prepareStatement("upsert into  "+ table+"(mykey,mycolumn) values(?,?)");
//			for(int i =0; i < 10000; i ++){
//				batchInsert.setInt(1, i);
//				batchInsert.setString(2, "batch "+i);
//				batchInsert.addBatch();
//			}
			long b = System.currentTimeMillis();
			
			// 批量提交
//			batchInsert.executeBatch();
//			con.commit();
//			System.out.println("批量添加 执行时间:"+String.valueOf(System.currentTimeMillis() - b));
			
			PreparedStatement statement = con.prepareStatement("select * from "+ table+" limit 2");
			rset = statement.executeQuery();
			while (rset.next()) {
				System.out.println("---------------" + rset.getString("APPLICATION_ID"));
//				Arrays.sort(rset.getArray(1));
			}
			
			statement.close();
			//--------------------------查询表结构------------------------------
//			DatabaseMetaData dmd = con.getMetaData().get;
//			ResultSet rs = dmd.getTables(null,"%", "TEST", new String[]{"TABLE"});
//			while(rs.next()){
//				String tabName = rs.getString("TABLE_NAME");
//				
//				System.out.println(tabName);
//				
//			}
			
			
			con.close();
			System.out.println("---------------执行完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
