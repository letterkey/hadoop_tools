package test.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

public class ParquetTest  {
	private static final Log LOG = Log.getLog(ParquetTest.class);
	
	public static class Map extends Mapper<LongWritable, Group, Void, Group> {
		@Override
		public void map(LongWritable key, Group value, Context context)
				throws IOException, InterruptedException {
			context.write(null, value);
		}
	}

	public static void main(String[] args) throws Exception {
		try {
			args = new String[]{"hdfs://localhost:9000/tmp/x.txt","hdfs://localhost:9000/out"};
			if (args.length < 2) {
				LOG.error("Usage: INPUTFILE OUTPUTFILE [compression]");
			}
			String inputFile = args[0];
			String outputFile = args[1];
			String compression = (args.length > 2) ? args[2] : "none";

			Configuration conf = new Configuration();
			
			Path parquetFilePath = new Path(inputFile);
			// Find a file in case a directory was passed
//			RemoteIterator<LocatedFileStatus> it = FileSystem.get(conf).listFiles(new Path(inputFile), true);
//			while (it.hasNext()) {
//				FileStatus fs = it.next();
//				if (fs.isFile()) {
//					parquetFilePath = fs.getPath();
//					break;
//				}
//			}
			if (parquetFilePath == null) {
				LOG.error("No file found for " + inputFile);
			}
			LOG.info("Getting schema from " + parquetFilePath);
			ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, parquetFilePath);
			MessageType schema = readFooter.getFileMetaData().getSchema();
			LOG.info(schema);
			GroupWriteSupport.setSchema(schema, conf);

			Job job = new Job(conf);
			job.setJarByClass(ParquetTest.class);
			job.setJobName("create parquet file");
			job.setMapperClass(Map.class);
			job.setNumReduceTasks(0);
			job.setInputFormatClass(ExampleInputFormat.class);
			job.setOutputFormatClass(ExampleOutputFormat.class);

			CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
			if (compression.equalsIgnoreCase("snappy")) {
				codec = CompressionCodecName.SNAPPY;
			} else if (compression.equalsIgnoreCase("gzip")) {
				codec = CompressionCodecName.GZIP;
			}
			LOG.info("Output compression: " + codec);
			ExampleOutputFormat.setCompression(job, codec);

			FileInputFormat.setInputPaths(job, parquetFilePath);
			FileOutputFormat.setOutputPath(job, new Path(outputFile));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
}
