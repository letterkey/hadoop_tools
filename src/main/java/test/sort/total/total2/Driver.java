package test.sort.total.total2;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import study.utils.EJob;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		if (arg0.length != 3) {
			System.err
					.println("Usage:\nfz.partitioner.PartitionerDriver <in> <out> <useTotalOrder>");
			return -1;
		}
		// System.out.println(conf.get("fs.defaultFS"));
		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		out.getFileSystem(conf).delete(out, true);
		Job job = Job.getInstance(conf, "total order partitioner");
		

	    File jarFile = EJob.createTempJar("bin");
	    EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
	    ClassLoader classLoader = EJob.getClassLoader();
	    Thread.currentThread().setContextClassLoader(classLoader);
	    ((JobConf) job.getConfiguration()).setJar(jarFile.toString()); 
	    
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(PartitionerMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		// System.out.println(job.getConfiguration().get("mapreduce.job.reduces"));
		// System.out.println(conf.get("mapreduce.job.reduces"));
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// reducer全局排序
		if (arg0[2] != null && "true".equals(arg0[2])) {
			job.setPartitionerClass(TotalOrderPartitioner.class);
			// InputSampler.Sampler<Text, Text> sampler = new
			// InputSampler.RandomSampler<Text, Text>(0.1,20,3);
			// InputSampler.writePartitionFile(job, sampler);
			
			MyInputSampler.Sampler<Text, Text> sampler = new MyInputSampler.RandomSampler<Text, Text>(0.1, 100, 3);
			MyInputSampler.writePartitionFile(job, sampler);
			
			String partitionFile = TotalOrderPartitioner.getPartitionFile(getConf());
			URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
			job.addCacheArchive(partitionUri);
		}

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		// ToolRunner.run(new Configuration(), new Driver(), args);

		String[] arg = new String[] { "hdfs://localhost:9000/tmp/input/",
				"hdfs://localhost:9000/tmp/out", 
				"true" };
		ToolRunner.run(new Configuration(), new Driver(), arg);
	}
}
