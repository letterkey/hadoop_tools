package infobright;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * 自定义读取器
 * @author YMY
 */
public class CustomRecordReader extends RecordReader<LongWritable, Text>  {
private LineReader lr ;
	
	private int index;
	
	private long start;

	private long end;

	private long currPos;
	
	private Text line = new Text();
	
	private LongWritable key = new LongWritable();
	
	private Text value = new Text();
	
    public CustomRecordReader(CombineFileSplit split,
    		TaskAttemptContext cxt,Integer index){  
        this.index=index;
    }
    
	@Override
	public void close() throws IOException {
		lr.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}
	
	// 跟踪读取分片的进度，这个函数就是根据已经读取的K-V对占总K-V对的比例来显示进度的
	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currPos - start) / (float) (end - start));  
        }
	}
	
	// initialize函数主要是计算分片的始末位置，以及打开想要的输入流以供读取K-V对，输入流另外处理分片经过压缩的情况
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext cxt)
			throws IOException, InterruptedException {
		CombineFileSplit split = (CombineFileSplit) inputSplit;
		FileSplit fileSplit = new FileSplit(
				split.getPath(index), 
				split.getOffset(index), 
				split.getLength(), 
				split.getLocations()
				);
		
		Configuration conf = cxt.getConfiguration();
		
		Path path = fileSplit.getPath();
		
		FileSystem fs = path.getFileSystem(conf);
		
		FSDataInputStream is = fs.open(path);
		
		lr = new LineReader(is, conf);
		
		start = fileSplit.getStart();
		
		end = start + fileSplit.getLength();
		
		is.seek(start);
		
		
		if(start != 0){
			// Math.min(Integer.MAX_VALUE, end-start) 返回较小的一个
			start += lr.readLine(
					new Text(), 
					0, 
					(int)Math.min(Integer.MAX_VALUE, end-start)
					);
		}
		currPos = start;
	}
	
	// 获取分片上的下一个K-V 对
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(currPos >= end)
			return false;
		currPos = lr.readLine(line);
		
		if(line.toString().length() == 0)
			return false;
		key.set(currPos);
		value.set(line);
		
		return true;
	}
}
