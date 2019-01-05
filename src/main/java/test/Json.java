package test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Test;

public class Json {
	@Test
	public void parseString() {
		try {
//			String data = "[['237','945261'],['Android','4.0.4','OPPO U705T','Android Agent','1.0.5']]";
//			ObjectMapper om = new ObjectMapper();
//			JsonNode root = om.readTree(data);
//			
//			System.out.println(root.path(0));
			String tmp = "1^A1422719998^A236^A490^A5^A13^A3^A2081^A16657^A1^A9^A0^A0^A631625";
			System.out.println(tmp.replaceAll("\\^A", "\t"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void excuteLoadData() {
		String tmp = "1	1422719998	236	490	5	13	3	2081	16657	1	9	0	0	631625";
		
		InputStream is = new ByteArrayInputStream(tmp.getBytes());
        Connection c = null;
        PreparedStatement p = null;
        try {
        	String sql ="load  data local infile '//opt//x.txt' IGNORE into table session_1 fields terminated by '\t'  lines terminated by '\n' ";
        	c = DriverManager.getConnection("jdbc:mysql://localhost:5029/mobileprivatedata", "root","000000");
            p = c.prepareStatement(sql);
            if (p.isWrapperFor(com.mysql.jdbc.Statement.class)) {
                com.mysql.jdbc.PreparedStatement mysqlStatement = p.unwrap(com.mysql.jdbc.PreparedStatement.class);
                mysqlStatement.setLocalInfileInputStream(is);
               mysqlStatement.executeUpdate();
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
	public void compareTo() {
		Long one = 1l;
		Long two = 2l;
		System.out.println(two.compareTo(one));
		
		
		String[] a = {"a","b","c"};
        String[] b = Arrays.copyOf(a,a.length - 1);
        String[] c = Arrays.copyOfRange(a, 1, a.length);
        for(String x : c)
        System.out.println(x);
		
    }
	
}
