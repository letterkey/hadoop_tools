package originallog;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import study.utils.EJob;

public class Driver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

//		conf.set("fs.defaultFS", "hdfs://localhost:9000");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.address", "localhost:8032");
//		conf.set("mapred.remote.os", "Linux");
//		conf.set("hadoop.job.ugi", "hadoop,hadoop");
		conf.set("maprd.jar","/hadoop");
	    Job job = new Job(conf, "word count");
//	    job.setJar("Driver.jar");
	    
	    File jarFile = EJob.createTempJar("bin");
	    EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
	    ClassLoader classLoader = EJob.getClassLoader();
	    Thread.currentThread().setContextClassLoader(classLoader);
	    ((JobConf) job.getConfiguration()).setJar(jarFile.toString()); 
	    
	    Path out = new Path("hdfs://localhost:9000/out");	// 输出路径
	    out.getFileSystem(conf).delete(out, true);	// 	删除原有输出目录
	    
//	    job.setJarByClass(Driver.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/t/*"));
		FileOutputFormat.setOutputPath(job, out);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
