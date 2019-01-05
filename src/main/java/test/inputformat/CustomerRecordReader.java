package test.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class CustomerRecordReader extends RecordReader<Text, Text> {
	private LineReader lr ;
    private Text key = new Text();
    private Text value = new Text();
    private long start ;
    private long end;
    private long currentPos;
    private Text line = new Text();
    
	@Override
	public void close() throws IOException {
		lr.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {  
            return 0.0f;  
        } else {  
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));  
        }  
	}
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext cxt)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = cxt.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream is = fs.open(path);
		
		lr = new LineReader(is,conf);
		start = split.getStart();
		
		end = start + split.getLength();
		is.seek(start);
		if(start != 0){
			// Math.min(Integer.MAX_VALUE, end-start) 返回较小的一个
			start += lr.readLine(new Text(), 0, (int)Math.min(Integer.MAX_VALUE, end-start));
		}
		currentPos = start;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(currentPos > end ){
			return false;
		}
		currentPos = lr.readLine(line);
		if(line.getLength() == 0)
			return false;
		if(line.toString().startsWith("ignore")){
			currentPos += lr.readLine(line);
		}
		
		String[] words = line.toString().split(",");
		if(words.length < 2){
			System.err.println("line:"+line.toString()+".");
			return false;
		}
		key.set(words[0]);
		value.set(words[1]);
		return true;
	}

}
