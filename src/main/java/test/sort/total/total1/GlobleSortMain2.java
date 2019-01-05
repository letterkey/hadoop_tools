package test.sort.total.total1;

import java.io.File;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import study.utils.EJob;

public class GlobleSortMain2 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Path in = new Path("hdfs://localhost:9000/tmp/in"); // 数据输入路径
		Path out = new Path("hdfs://localhost:9000/tmp/out"); // 输出路径
		out.getFileSystem(conf).delete(out, true); // 删除原有输出目录

		Job job = new Job(conf, "globle sort 2");

	    File jarFile = EJob.createTempJar("bin");
	    EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
	    ClassLoader classLoader = EJob.getClassLoader();
	    Thread.currentThread().setContextClassLoader(classLoader);
	    ((JobConf) job.getConfiguration()).setJar(jarFile.toString()); 
	    
		job.setJarByClass(GlobleSortMain2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.RandomSampler<LongWritable, NullWritable> sampler = new InputSampler.RandomSampler<LongWritable, NullWritable>(
				0.1, 100, 3);
		
		Path partitionFile = new Path(in, "_sortpartitions");
		
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

		InputSampler.writePartitionFile(job, sampler);

		// 一般都将该文件做distribute cache处理
		URI partitionURI = new URI(partitionFile.toString()
				+ "#_sortpartitions");
		// 从上面可以看出 采样器是在map阶段之前进行的
		// 在提交job的client端完成的
		DistributedCache.addCacheFile(partitionURI, conf);
		DistributedCache.createSymlink(conf);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");

	}
}
