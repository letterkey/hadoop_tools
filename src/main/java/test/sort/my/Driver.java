package test.sort.my;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("df.default.name", "hdfs://master:9000/");
		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapred.job.tracker", "master:9001");
		conf.set("mapred.min.split.size", "64");

		String[] ars = new String[] { "hdfs://master:9000/usr/input/s*.txt",	
				"hdfs://master:9000/usr/output/"};
		String[] otherArgs = new GenericOptionsParser(conf, ars)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "second sort");
		
		Path out = new Path(otherArgs[1]);
		out.getFileSystem(conf).delete(out,true);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		job.setGroupingComparatorClass(Group.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(NullWritable.class);
//		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, out);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
