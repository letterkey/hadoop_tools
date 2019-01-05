package test.sequence;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import test.EJob;

public class MapReduceReadFile {

	private static SequenceFile.Reader reader = null;
	private static Configuration conf = new Configuration();

	public static class ReadFileMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		// private SequenceFile.Reader reader = null;

		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path path = ((FileSplit) context.getInputSplit()).getPath();
			FileSystem fs = path.getFileSystem(conf);
			reader = new SequenceFile.Reader(fs, path, conf);
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) {
			key = (LongWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			BytesWritable v = (BytesWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			try {
				//此处要注意,每次map,将会有多条记录,此处用循环遍历
				while (reader.next(key, v)) {
//					System.out.printf("%s\t%s\n", key, new String(v.getBytes()));
//					context.write(key, new Text(value.getBytes()));
//					String[] sv = new String(v.getBytes()).split("\t");
//					String tmp="";
//					for(String e : sv){
//						tmp=tmp +e+"\\t";
//					}
					byte[] tmpv = new byte[v.getLength()];
					System.arraycopy(v.getBytes(), 0, tmpv, 0, v.getLength());
					
					
					System.out.println("-------------------"+new String(tmpv));
					context.write(key, new Text(new String(tmpv)+"\n"));
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReadFileReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String sum = "";
			for (Text val : values) {
				sum += val.toString();
			}
			context.write(key, new Text(sum));
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = new Job(conf, "mr-sequencefile");

		File jarFile = EJob.createTempJar("target");
		EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
		ClassLoader classLoader = EJob.getClassLoader();
		Thread.currentThread().setContextClassLoader(classLoader);
		((JobConf) job.getConfiguration()).setJar(jarFile.toString());

		job.setJarByClass(MapReduceReadFile.class);
		job.setMapperClass(ReadFileMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReadFileReducer.class);
		
		Path path = new Path("/test/metricdata.seq");
		FileSystem fs = FileSystem.get(conf);
		FileInputFormat.addInputPath(job, path);

		Path out = new Path("/out/seq/");
		if(fs.exists(out))
			fs.delete(out, true);
		
		FileOutputFormat.setOutputPath(job,out );
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}