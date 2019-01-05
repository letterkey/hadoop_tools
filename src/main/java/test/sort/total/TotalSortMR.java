package test.sort.total;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * 全局排序 测试
 * @author YMY
 */
public class TotalSortMR {

	public static int runTotalSortJob(String[] args) throws Exception {
		Path partitionFile = new Path(args[2]);
		int reduceNumber = Integer.parseInt(args[3]);
		
		// RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input
		// splits数
		RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);

		Configuration conf = new Configuration();
		Path in = new Path("/tmp/*.txt");		// 数据输入路径
		Path out = new Path("/tmp/out");	// 输出路径
	    out.getFileSystem(conf).delete(out, true);	// 	删除原有输出目录
	    
		// 设置partition file全路径到conf
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		Job job = new Job(conf);
		job.setJobName("Total-Sort");
		job.setJarByClass(TotalSortMR.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(reduceNumber);
		
		// partitioner class设置成TotalOrderPartitioner
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// 写partition file到mapreduce.totalorderpartitioner.path
		InputSampler.writePartitionFile(job, sampler);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		System.exit(runTotalSortJob(args));
	}
}
