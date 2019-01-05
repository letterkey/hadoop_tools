package test.phoenix;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
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

public class Tmp {

	private static Configuration conf = new Configuration();

	public static class ReadFileMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		private static SequenceFile.Reader reader = null;
		// private SequenceFile.Reader reader = null;
		private Connection con = null;
		private static PreparedStatement batchInsert = null;
		private String[] vs = null;
		private final static IntWritable one = new IntWritable(0);

		@Override
		protected void setup(
				Mapper<LongWritable, Text, LongWritable, Text>.Context context) {
			try {
				Configuration conf = context.getConfiguration();
				Path path = ((FileSplit) context.getInputSplit()).getPath();
				FileSystem fs = path.getFileSystem(conf);
				reader = new SequenceFile.Reader(fs, path, conf);

				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
				con = DriverManager.getConnection("jdbc:phoenix:localhost");
				batchInsert = con
						.prepareStatement("upsert into machinemeasurement_201500506 (timestamp, app_id, app_version_id, count, total, exclusive, min, max, sum_of_squares, os_id, os_version_id, device_type_id, manufacturer_id, model_id, name_type_id, name_id, device_id)"
								+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

				super.setup(context);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) {
			try {
				key = (LongWritable) ReflectionUtils.newInstance(
						reader.getKeyClass(), conf);
				BytesWritable v = (BytesWritable) ReflectionUtils.newInstance(
						reader.getValueClass(), conf);

				System.out.println("=====================================================");
				key = (LongWritable) ReflectionUtils.newInstance(
						reader.getKeyClass(), context.getConfiguration());
				BytesWritable bv = (BytesWritable) ReflectionUtils.newInstance(
						reader.getValueClass(), context.getConfiguration());
				while (reader.next(key, bv)) {
					one.set(one.get() + 1);
					if (bv.getSize() > 0) {
						byte[] tmpv = new byte[bv.getLength()];
						System.arraycopy(bv.getBytes(), 0, tmpv, 0, bv.getLength());
						
						value = new Text(new String(tmpv));

						if (StringUtils.isNotEmpty(value.toString())) {
							System.out.println(value.toString()+ "--------------------------------------");
							vs = StringUtils.split(value.toString(), "\t");

							batchInsert.setLong(1, Long.valueOf(vs[0]) / 1000L);
							batchInsert.setInt(2, Integer.valueOf(vs[7]));
							batchInsert.setInt(3, Integer.valueOf(vs[8]));

							batchInsert.setDouble(4, Double.valueOf(vs[1]));
							batchInsert.setDouble(5, Double.valueOf(vs[2]));
							batchInsert.setDouble(6, Double.valueOf(vs[3]));
							batchInsert.setDouble(7, Double.valueOf(vs[4]));
							batchInsert.setDouble(8, Double.valueOf(vs[5]));
							batchInsert.setDouble(9, Double.valueOf(vs[6]));

							batchInsert.setInt(10, Integer.valueOf(vs[9]));
							batchInsert.setInt(11, Integer.valueOf(vs[10]));

							batchInsert.setInt(12, Integer.valueOf(vs[11]));
							batchInsert.setInt(13, Integer.valueOf(vs[12]));
							batchInsert.setInt(14, Integer.valueOf(vs[13]));
							batchInsert.setInt(15, Integer.valueOf(vs[14]));
							batchInsert.setInt(16, Integer.valueOf(vs[15]));
							batchInsert.setInt(17, Integer.valueOf(vs[16]));

							batchInsert.addBatch();
							if (one.get() >= 3000) {
								batchInsert.executeBatch();
								con.commit();
								one.set(0);
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public static class ReadFileReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
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

		job.setJarByClass(Tmp.class);
		job.setMapperClass(ReadFileMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReadFileReducer.class);

		Path path = new Path("/test/measurement.seq");
		FileSystem fs = FileSystem.get(conf);
		FileInputFormat.addInputPath(job, path);

		Path out = new Path("/out/seq/");
		if (fs.exists(out))
			fs.delete(out, true);

		FileOutputFormat.setOutputPath(job, out);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}