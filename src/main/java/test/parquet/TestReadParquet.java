package test.parquet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.example.ExampleInputFormat;
import test.EJob;
import test.SecondarySort;

public class TestReadParquet  {
	private static final Log LOG = Log.getLog(TestReadParquet.class);

	private static class FieldDescription {
		public String constraint;
		public String type;
		public String name;
	}

	private static class RecordSchema {
		public RecordSchema(String message) {
			fields = new ArrayList<FieldDescription>();
			List<String> elements = Arrays.asList(message.split("\n"));
			Iterator<String> it = elements.iterator();
			while (it.hasNext()) {
				String line = it.next().trim().replace(";", "");
				System.err.println("RecordSchema read line: " + line);
				if (line.startsWith("optional") || line.startsWith("required")) {
					String[] parts = line.split(" ");
					FieldDescription field = new FieldDescription();
					field.constraint = parts[0];
					field.type = parts[1];
					field.name = parts[2];
					fields.add(field);
				}
			}
		}

		private List<FieldDescription> fields;

		public List<FieldDescription> getFields() {
			return fields;
		}
	}

	/*
	 * Read a Parquet record, write a CSV record
	 */
	public static class ReadRequestMap extends Mapper<LongWritable, Group, NullWritable, Text> {
		private static List<FieldDescription> expectedFields = null;

		@Override
		public void map(LongWritable key, Group value, Context context)
				throws IOException, InterruptedException {
			NullWritable outKey = NullWritable.get();
			if (expectedFields == null) {
				// Get the file schema which may be different from the fields in
				// a particular record) from the input split
				ParquetInputSplit pq = (ParquetInputSplit) context.getInputSplit();
//				String fileSchema = pq.get
//				RecordSchema schema = new RecordSchema(fileSchema);
//				expectedFields = schema.getFields();
			}

			// No public accessor to the column values in a Group, so extract
			// them from the string representation
			String line = value.toString();
			String[] fields = line.split("\n");

			StringBuilder csv = new StringBuilder();
			boolean hasContent = false;
			int i = 0;
			// Look for each expected column
			Iterator<FieldDescription> it = expectedFields.iterator();
			while (it.hasNext()) {
				if (hasContent) {
					csv.append(',');
				}
				String name = it.next().name;
				if (fields.length > i) {
					String[] parts = fields[i].split(": ");
					// We assume proper order, but there may be fields missing
					if (parts[0].equals(name)) {
						boolean mustQuote = (parts[1].contains(",") || parts[1]
								.contains("'"));
						if (mustQuote) {
							csv.append('"');
						}
						csv.append(parts[1]);
						if (mustQuote) {
							csv.append('"');
						}
						hasContent = true;
						i++;
					}
				}
			}
			context.write(outKey, new Text(csv.toString()));
		}
	}

	public static void  main(String[] args) throws Exception {
		args = new String[]{"/test/data","/out/parquet/"};
		
		Configuration conf = new Configuration();
//		conf.set("df.default.name", "hdfs://localhost:9000/");
		Job job = new Job(conf, "read parquet");
		
		File jarFile = EJob.createTempJar("bin");
		EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
		ClassLoader classLoader = EJob.getClassLoader();
		Thread.currentThread().setContextClassLoader(classLoader);
		JobConf jc = ((JobConf) job.getConfiguration());
		jc.setJar(jarFile.toString());
		job.setJarByClass(TestReadParquet.class);
		job.setJobName("test parquet");

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ReadRequestMap.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
