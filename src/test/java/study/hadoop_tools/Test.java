package study.hadoop_tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) {
		final List<String> list = new ArrayList<String>();
		list.add("hello");
		list.add("helloabc");
		list.add("hello123");
		list.add("hellob23");
		boolean con =list.contains("hello[a-z]*");
		
		final List<String> friends =
				Arrays.asList("Brian", "Nate", "Neal", "Raju", "Sara", "Scott");
		
		
		String x="abc";
		System.out.println(x.indexOf("c"));
		System.out.println(con);
		
	}

}
