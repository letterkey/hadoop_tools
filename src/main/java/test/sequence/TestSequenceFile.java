package test.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class TestSequenceFile {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// // TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		 Path seqFile = new Path("/test/seqFile2.seq");
		 // Writer内部类用于文件的写操作,假设Key和Value都为Text类型
		 SequenceFile.Writer writer = SequenceFile.createWriter(conf,
		 Writer.file(seqFile), Writer.keyClass(Text.class),
		 Writer.valueClass(Text.class),
		 Writer.compression(CompressionType.NONE));

		 // 通过writer向文档中写入记录
		 writer.append(new Text("key"), new Text("value"));
		
		 IOUtils.closeStream(writer);// 关闭write流
		 // 通过reader从文档中读取记录
		 SequenceFile.Reader reader = new SequenceFile.Reader(conf,
		 Reader.file(seqFile));
		 Text key = new Text();
		 Text value = new Text();
		 while (reader.next(key, value)) {
		 System.out.println(key);
		 System.out.println(value);
		 }
		 IOUtils.closeStream(reader);// 关闭read流

		
	}

}