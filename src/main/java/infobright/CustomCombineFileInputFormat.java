package infobright;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * 自定义文件输入:多文件输入合并
 * @author YMY
 *
 */
public class CustomCombineFileInputFormat extends CombineFileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,TaskAttemptContext cxt) throws IOException {
		
		return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split,cxt,CustomRecordReader.class);  
	}
	
	
	/**
	 * 根据业务设置此path是否让被切分，用于用户希望一个文件被一个map处理
	 */
	@Override
	public boolean isSplitable(JobContext context, Path file){
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
		
	}
}
