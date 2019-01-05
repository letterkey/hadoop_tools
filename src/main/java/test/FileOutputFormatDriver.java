package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.outputformat.CustomerOutPutFormat;

/**
 * 测试自定义输入格式
* @Title: FileOutputFormatDriver.java
* @author YMY
* @date 2015年2月14日 下午3:14:59 
* @version V1.0
 */
public class FileOutputFormatDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		String[] arg = {
				"hdfs://master:9000/usr/input/nginx",
				"hdfs://master:9000/usr/output/inputFormat" 
		};
		args = arg;
		ToolRunner.run(new Configuration(), new FileOutputFormatDriver(),args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if(arg0.length!=3){
			System.err.println("Usage:\nfz.outputformat.FileOutputFormatDriver <in> <out> <numReducer>");
			return -1;
		}
		
		Configuration conf = getConf();
		conf.set("df.default.name", "hdfs://master:9000/");
//		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapred.job.tracker", "master:9001");
		conf.set("mapred.min.split.size", "64");
		
		Path in = new Path(arg0[0]);
		Path out= new Path(arg0[1]);
		
		// 删除已经存在的输出目录
		boolean delete=out.getFileSystem(conf).delete(out, true);
		System.out.println("deleted "+out+"?"+delete);
		Job job = Job.getInstance(conf,"fileouttputformat test job");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		
		// 自定义输出格式
		job.setOutputFormatClass(CustomerOutPutFormat.class);
		
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(arg0[2]));
		job.setReducerClass(Reducer.class);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true)?0:-1;
	}

}
