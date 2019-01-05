package test;

import org.junit.Test;

public class CLITest{
	@Test
	 public void testHelp() { 
		 String args[]={"-h"}; 
		 CLI.simpleTest(args);
	 } 

	 public void testNoArgs() { 
		 String args[] = new String[0]; 
		 CLI.simpleTest(args);
	 } 

	 public void testRMDataSource() { 
		 String args[] = new String[]{"-i","192.168.0.2","-p","5988","-t","http"}; 
		 CLI.simpleTest(args);
	 }
}
