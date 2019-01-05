package test.parquet;

import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REQUIRED;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import parquet.hadoop.ParquetOutputFormat;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

@SuppressWarnings("deprecation")
public class App extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Void, ArrayWritable> {
		Logger LOG = Logger.getLogger(MyMapper.class);

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			DataWritableWriteSupport.getSchema(context.getConfiguration());
			MessageType mt = DataWritableWriteSupport.getSchema(context.getConfiguration());
			System.out.println("-------------"+mt.toString());
		}

		@Override
		public void map(LongWritable n, Text line, Context context)
				throws IOException, InterruptedException {
			if (line != null && line.getLength() > 0) {
				String[] parts = line.toString().split(" ");
				Writable[] data = new Writable[2];
				for (int i = 0; i < 2; i++) {
					data[i] = new BinaryWritable(Binary.fromString(parts[i]));
				}
				ArrayWritable aw = new ArrayWritable(Writable.class, data);
				context.write(null, aw);
			}
		}
	}

	public int run(String[] args) throws Exception {
		args = new String[] { "hdfs://localhost:9000/tmp/test.txt",
				"hdfs://localhost:9000/out" };

		Configuration conf = new Configuration();

		Path inpath = new Path(args[0]);
		Path outpath = new Path(args[1]);
		FileSystem fs = outpath.getFileSystem(conf);
		if(fs.exists(outpath))
			fs.delete(outpath, true);
		MessageType mt = new MessageType("people", new PrimitiveType(REQUIRED,
				BINARY, "name"), new PrimitiveType(OPTIONAL, BINARY, "city"));

		DataWritableWriteSupport.setSchema(mt, conf);

		System.out.println("Schema: " + mt.toString());

		Job job = new Job(conf, "parquet-convert");

		job.setJarByClass(getClass());
		job.setJobName("parquet-convert");
//		job.setMapOutputKeyClass(Void.class);
		job.setMapOutputValueClass(ArrayWritable.class);
//		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(ArrayWritable.class);
		job.setMapperClass(App.MyMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ParquetOutputFormat.class);

		FileInputFormat.setInputPaths(job, inpath);
		ParquetOutputFormat.setOutputPath(job, outpath);

		ParquetOutputFormat.setWriteSupportClass(job, DataWritableWriteSupport.class);

		return (job.waitForCompletion(true)) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new App(), args));
	}

}