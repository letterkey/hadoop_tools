package infobright;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import study.utils.EJob;

public class InfoBright {
	public static class Map extends Mapper<LongWritable, Text, CustomKey, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("map接受数据:"+value.toString());
			if(StringUtils.isNotEmpty(value.toString())){
				try{
					String[] k = value.toString().split("\\^A");
					if(k.length>=3){
						CustomKey ck = new CustomKey(Long.valueOf(k[1]),Long.valueOf(k[2]),Long.valueOf(k[3]));
						System.out.println(ck);
						context.write(ck, value);
					}
				}catch(Exception e){
					System.out.println("map阶段数据格式错误:"+value.toString());
				}
			}
		}
	}
	public static class MyReduce extends Reducer<CustomKey, Text, CustomKey, Text> {
		public void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 StringBuffer sb = new StringBuffer();
			for (Text val : values) {
				sb.append(val.toString()).append("\n");
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	
	/**
	 *@author YMY 
	 *@date 2015年1月28日 上午10:10:21 
	 *@param args
	 *@throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args) .getRemainingArgs();
//		if (otherArgs.length < 7) {
//			System.err.println("Usage: infobright <in> [<in>...] <out>[out]<dbserver>[dbserver]"
//					+ "<username>[username]<password>[password]<table>[table]<stepsize>[stepsize]");
//			System.exit(2);
//		 }
		
//		Path in = new Path(otherArgs[0]);		// 数据输入路径
//		Path out = new Path(otherArgs[1]);	// 输出路径
//	    out.getFileSystem(conf).delete(out, true);	// 	删除原有输出目录
//	    conf.set("dbserver", otherArgs[2]);
//	    conf.set("username", otherArgs[3]);
//	    conf.set("password", otherArgs[4]);
//	    conf.set("table", otherArgs[5]);
//	    conf.set("stepsize", otherArgs[6]);

		Path in = new Path("/tmp/*.txt");		// 数据输入路径
		Path out = new Path("/tmp/out");	// 输出路径
	    out.getFileSystem(conf).delete(out, true);	// 	递归删除原有输出目录
	    conf.set("dbserver", "localhost:5029/mobileprivatedata");
	    conf.set("username", "root");
	    conf.set("password", "000000");
	    conf.set("table", "session_1");
	    conf.set("stepsize", "1000");
	    
		Job job = new Job(conf, "hdfs_to_infobright");
		
	    File jarFile = EJob.createTempJar("bin");
	    EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
	    ClassLoader classLoader = EJob.getClassLoader();
	    Thread.currentThread().setContextClassLoader(classLoader);
	    ((JobConf) job.getConfiguration()).setJar(jarFile.toString()); 
	    
		job.setJarByClass(InfoBright.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(Text.class);
		
//		job.setOutputFormatClass(InfoBrightOutPutFormat.class);
		
		job.setInputFormatClass(CustomCombineFileInputFormat.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// 使用TotalOrderPartitioner 分区
		job.setPartitionerClass(TotalOrderPartitioner.class);
		// 随机抽样,效率最低
		MyInputSampler.Sampler<Text, Text> sampler = new MyInputSampler.RandomSampler<Text, Text>(0.1, 5, 100);
		MyInputSampler.Sampler<Text, Text> s1 =  new MyInputSampler.SplitSampler(4);
		MyInputSampler.writePartitionFile(job, s1);
		
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		job.addCacheArchive(partitionUri);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
