package test.counter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	@Override
	public void map(LongWritable key,Text value,Context cxt){
		try{
			String[] values = value.toString().split(",");
			if(values.length > 1){
				cxt.write(new Text("-"+values[0]), new Text(values[1]));
			}
			
			// 统计记录 自定义counter
			cxt.getCounter(MY_COUNTER.CORRUPTED_DATA_COUNTER).increment(1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
	}
}
