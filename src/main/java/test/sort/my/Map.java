package test.sort.my;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, TextPair, NullWritable> {
	
	public void map(LongWritable key,Text value,Context cxt){
		try{
			String[] values = value.toString().split(",");
			if(values.length > 1){
				TextPair tp = new TextPair(values[0],values[1]);
				cxt.write(tp, NullWritable.get());
			}
			
			cxt.getCounter("", "").increment(1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
}
