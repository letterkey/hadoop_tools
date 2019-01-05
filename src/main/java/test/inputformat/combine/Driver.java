package test.inputformat.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.counter.MY_COUNTER;


/**
 * 测试自定义输出格式
 * @author YHT
 *
 */
public class Driver extends Configured implements Tool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String[] arg = {
				"hdfs://master:9000/usr/input/inputformat*.txt",
				"hdfs://master:9000/usr/output/combineInputFormat" 
		};
		args = arg;
		ToolRunner.run(new Configuration(), new Driver(),args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if(arg0.length!=2){
			System.err.println("Usage:\nfz.outputformat.FileOutputFormatDriver <in> <out> <numReducer>");
			return -1;
		}
		Configuration conf = getConf();
		
		conf.set("df.default.name", "hdfs://master:9000/");
//		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapred.job.tracker", "master:9001");
//		conf.set("mapred.min.split.size", "64");
		
		Path in = new Path(arg0[0]);
		Path out= new Path(arg0[1]);
		
		// 删除已经存在的输出目录
		boolean delete=out.getFileSystem(conf).delete(out, true);
		System.out.println("deleted "+out+"?"+delete);
		Job job = Job.getInstance(conf,"fileouttputformat test job");
		job.setJarByClass(getClass());
		
		// 自定义输入格式
		job.setInputFormatClass(CustomCombineFileInputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(Reduce.class);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		int flag = job.waitForCompletion(true)?0:-1;
		
		Counters counters = job.getCounters();
		Counter counter = counters.findCounter(MY_COUNTER.CORRUPTED_DATA_COUNTER);
		System.out.println("--------------------"+counter.getValue()+"-------------------");
		return flag;
	}

}
