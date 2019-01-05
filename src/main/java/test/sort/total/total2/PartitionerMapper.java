package test.sort.total.total2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionerMapper extends Mapper<LongWritable,Text,LongWritable ,LongWritable>{
	
	private Text newKey= new Text();
	private Text newValue = new Text();
	public void map(LongWritable key, Text value, Context cxt) throws IOException,InterruptedException{
//		String [] line =value.toString().split(" ");
//		if(line.length!=2){
//			return ;
//		}
//		newKey.set(line[1]);
//		newValue.set(line[0]);
//		cxt.write(newKey, newValue);
		cxt.write(new LongWritable(Long.valueOf(value.toString())),new LongWritable(Long.valueOf(value.toString())));
	}
}
