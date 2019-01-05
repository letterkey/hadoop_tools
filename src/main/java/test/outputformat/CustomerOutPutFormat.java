package test.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义CustomFileOutputFormat(把默认文件名前缀替换掉):
 * @author YHT
 */
public class CustomerOutPutFormat extends FileOutputFormat<LongWritable, Text> {
	private String prefix = "customer_";
	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Path outPut = FileOutputFormat.getOutputPath(job);
		String taskId = job.getTaskAttemptID().getTaskID().toString();
		Path path = new Path(outPut.toString()+"/"+prefix+taskId.substring(taskId.length()-5, taskId.length()));
		FSDataOutputStream fileOut = path.getFileSystem(job.getConfiguration()).create(path);  
        return new CustomRecordWriter(fileOut);  
	}

}
