package test.parquet;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class MyWriter {
	public static void main(String[] args) throws IOException {
		args = new String[]{"/opt/data/"};
		if (args.length < 1) {
			System.out.println("BasketWriter outFilePath");
			System.exit(0);
		}
		new MyWriter().generateBasketData(args[0]);
	}

	private void generateBasketData(String outFilePath) throws IOException {
		// 创建schema
		final MessageType schema = MessageTypeParser
				.parseMessageType(
						"message basket { "
						+ "required int64 basketid; "
						+ "required float price; "
						+ "required float totalbasketvalue; "
						+ "}"
						);
		
		Configuration config = new Configuration();
		DataWritableWriteSupport.setSchema(schema, config);
		File outDir = new File(outFilePath).getAbsoluteFile();
		Path outDirPath = new Path(outDir.toURI());
		FileSystem fs = outDirPath.getFileSystem(config);
		fs.delete(outDirPath, true);
		ParquetWriter writer = new ParquetWriter(outDirPath,
				new DataWritableWriteSupport() {
					@Override
					public WriteContext init(Configuration configuration) {
						if (configuration.get(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA) == null) {
							configuration.set(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA,
											schema.toString());
						}
						return super.init(configuration);
					}
				}, CompressionCodecName.SNAPPY, 256 * 1024 * 1024, 100 * 1024);
		Writable[] vs =new Writable[3];
		vs[0] = new LongWritable(1000);
		vs[1] = new FloatWritable(2.1f);
		vs[2] = new FloatWritable(2.2f);
		ArrayWritable v = new ArrayWritable(Writable.class, vs);
		writer.write(v);
		
		writer.close();
	}
}
