package test.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicDao {
	private static final Logger LOG = LoggerFactory.getLogger(BasicDao.class);
	private static Stack<String> stack = new Stack<String>();
	private static Map<String, Stack<String>> cache = new HashMap<String, Stack<String>>();
	public static Configuration conf;
	
	public static <E> boolean excuteLoadData(String data) {
		try {
			String table = conf.get("table","SESSION");
			
			Statement stmt = null;
			ResultSet rset = null;

			Connection con = DriverManager.getConnection("jdbc:phoenix:dn.hdp.010104015209.tgz");
			stmt = con.createStatement();
			// String create = "create table " + table +
			// " (mykey integer not null primary key, mycolumn varchar)";
			// System.out.println(create);
			// stmt.executeUpdate(create);
			// stmt.executeUpdate("upsert into " + table +
			// " values (1,'Hello')");
			// stmt.executeUpdate("upsert into " + table +
			// " values (2,'World!')");
			stmt.executeUpdate(data);
			con.commit();
			//
			// PreparedStatement statement =
			// con.prepareStatement("select * from "
			// + table);
			// rset = statement.executeQuery();
			// while (rset.next()) {
			// System.out.println("---------------"
			// + rset.getString("mycolumn"));
			// }
			// statement.close();
			// con.close();
			System.out.println("---------------执行完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 压栈
	 * 
	 * @author YMY
	 * @date 2015年1月28日 下午1:41:53
	 * @param item
	 */
	public static void push(String tdate, String item) {
		if (!BasicDao.cache.containsKey(tdate))
			BasicDao.cache.put(tdate, new Stack<String>());
		BasicDao.cache.get(tdate).push(item);
		int stepsize = conf.getInt("stepsize", 1000);
		if (!BasicDao.cache.get(tdate).isEmpty()
				&& BasicDao.cache.get(tdate).size() == stepsize) {
			StringBuffer sb = new StringBuffer();
			String tmp = "";
			String prefix ="upsert into SESSION (timestamp,count,app_id,app_version_id,os_id,os_version_id,device_type_id,manufacturer_id,model_id,country_code_id,region_code_id,carries_id,net_type_id,device_id) values ";
			sb.append(prefix);
			int counter = 0;
			for (int i = 1; i <= stepsize; i++) {
				if (BasicDao.cache.get(tdate).isEmpty())
					continue;
				tmp = BasicDao.cache.get(tdate).pop().trim();
				sb.append(tmp).append(",");
				counter++;
			}
			String tmp1 =sb.toString();
			if(sb.toString().endsWith(","))
				tmp1 =tmp1.substring(0,tmp1.toString().length()-1);
			
			System.out.println("------------tmp1--------------"+tmp1);
			long t = System.currentTimeMillis();
			BasicDao.excuteLoadData(tmp1);
			long diff = System.currentTimeMillis() - t;
			Log.info("执行load导入[" + tdate + "]记录数[" + counter + "]条,用时[" + diff
					+ "]毫秒");
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
		Log.info("执行basicdao.clear方法isempty:" + BasicDao.stack.isEmpty()
				+ " size:" + BasicDao.stack.size());
		
		for (String key : BasicDao.cache.keySet()) {
			Stack<String> data = BasicDao.cache.get(key);
			if (data == null || data.isEmpty())
				continue;
			StringBuffer sb = new StringBuffer();
			String prefix ="upsert into SESSION (timestamp,count,app_id,app_version_id,os_id,os_version_id,device_type_id,manufacturer_id,model_id,country_code_id,region_code_id,carries_id,net_type_id,device_id) values ";
			sb.append(prefix);
			
			Enumeration<String> items = data.elements(); // 得到 stack 中的枚举对象
			int counter = 0;
			while (items.hasMoreElements()) { // 显示枚举（stack ） 中的所有元素
				String tmp = items.nextElement().trim();
				if (StringUtils.isNotEmpty(tmp)) {
					sb.append(tmp).append(",");
				}
				counter++;
			}
			String tmp1 =sb.toString();
			if(sb.toString().endsWith(","))
				tmp1 =tmp1.substring(0,tmp1.toString().length()-1);
			System.out.println("-------------clear-------------"+tmp1);
			long t = System.currentTimeMillis();
			BasicDao.excuteLoadData(tmp1);
			long diff = System.currentTimeMillis() - t;
			Log.info("clear 执行load导入[" + key + "]记录数[" + counter + "]条,用时["
					+ diff + "]毫秒");
		}
	}

}
