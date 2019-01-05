package test.sort.top.top1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
	private long tmp = Long.MIN_VALUE;


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
	}
	
	@Override
	public void map(LongWritable key,Text value,Context cxt){
		try{
			String[] values = value.toString().split(" ");
			if(values.length > 1){
				tmp = Math.max(tmp, Long.valueOf(values[1]));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new LongWritable(tmp), NullWritable.get());
	}
}
