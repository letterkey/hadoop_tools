package study.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author root
 *
 */
public class LocalJob extends Job {

	public LocalJob() throws IOException {
		super();
	}

	public LocalJob(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
	}

	public LocalJob(Configuration conf) throws IOException {
		super(conf);
	}
	
	public static LocalJob getInstance(Configuration conf, String jobName) throws IOException{
		JobConf jobConf = new JobConf(conf);
		LocalJob job=new LocalJob(jobConf);
		return job;
	}
	
	public void setJarByClass(Class<?> clazz){
		super.setJarByClass(clazz);
		
		conf.setJar("file:///opt/target/xxx.jar");
	}
	
}