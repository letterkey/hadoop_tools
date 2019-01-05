
package test;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{"hdfs://localhost:9000/test/test.txt","hdfs://localhost:9000/out/parquet/"};
		
		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
//		if (otherArgs.length < 2) {
//			System.err.println("Usage: wordcount <in> [<in>...] <out>");
//			System.exit(2);
//		}
		
//		DistributedCache.addFileToClassPath(new Path("/hdfsPath/mysql- connector-java- 5.1.0-bin.jar"), conf);
		Job job = new Job(conf, "word count test");
		
		File jarFile = EJob.createTempJar("target");
		EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
		ClassLoader classLoader = EJob.getClassLoader();
		Thread.currentThread().setContextClassLoader(classLoader);
		((JobConf) job.getConfiguration()).setJar(jarFile.toString());
		
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path p = new Path(args[0]);
		Path out = new Path(args[1]);
		FileSystem fs = out.getFileSystem(conf);
		if(fs.exists(out))
			fs.delete(out, true);
		
		FileInputFormat.setInputPaths(job, p);
		FileOutputFormat.setOutputPath(job, out);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
