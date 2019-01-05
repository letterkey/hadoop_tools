package test.outputformat;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.FileOutputFormatDriver;

/**
 * （根据key和value值输出数据到不同目录）：自定义主类（主类其实就是修改了输出的方式而已）
* @Title: MultipleOutputDriver.java
* @author YMY
* @date 2015年2月14日 下午3:17:12 
* @version V1.0
 */
public class MultipleOutputDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new FileOutputFormatDriver(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 3) {
			System.err
					.println("Usage:\nfz.multipleoutputformat.FileOutputFormatDriver <in> <out> <numReducer>");
			return -1;
		}
		Configuration conf = getConf();

		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		boolean delete = out.getFileSystem(conf).delete(out, true);
		System.out.println("deleted " + out + "?" + delete);
		Job job = Job.getInstance(conf, "fileouttputformat test job");
		job.setJarByClass(getClass());

		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(CustomOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "ignore", TextOutputFormat.class,
				LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "other", TextOutputFormat.class,
				LongWritable.class, Text.class);

		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(arg0[2]));
		job.setReducerClass(MultipleReducer.class);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}

/**
 * 自定义reducer（因为要根据key和value的值输出数据到不同目录，所以需要自定义逻辑）
 * @author Administrator
 *
 */
class MultipleReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	private MultipleOutputs<LongWritable, Text> out;

	@Override
	public void setup(Context cxt) {
		out = new MultipleOutputs<LongWritable, Text>(cxt);
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> value, Context cxt)
			throws IOException, InterruptedException {
		for (Text v : value) {
			if (v.toString().startsWith("ignore")) {
				// System.out.println("ignore--------------------value:"+v);
				out.write("ignore", key, v, "ign");
			} else {
				// System.out.println("other---------------------value:"+v);
				out.write("other", key, v, "oth");
			}
		}
	}

	@Override
	public void cleanup(Context cxt) throws IOException, InterruptedException {
		out.close();
	}
}