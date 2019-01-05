package test.sort.top.topk;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
	
	private long top[];
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		top = new long[3];
	}

	public void reduce(LongWritable key, Iterable<NullWritable> values, Context cxt) {
		try{
			top[0] = key.get();
			Arrays.sort(top);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		for(int i = 0; i < top.length; i ++){
			context.write(new LongWritable(top[i]), NullWritable.get());
		}
	}
	
	
	
}
