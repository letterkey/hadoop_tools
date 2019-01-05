package test.sort.top.topk;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
	private long[] top;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		top = new long[3];
	}
	
	@Override
	public void map(LongWritable key,Text value,Context cxt){
		try{
			String[] values = value.toString().split(" ");
			if(values.length > 1){
				// 每次替换最小的0位数据
				top[0] = Long.valueOf(values[1]);
				Arrays.sort(top);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(int i = 0; i < top.length; i ++){
			context.write(new LongWritable(top[i]), NullWritable.get());
		}
	}
}
