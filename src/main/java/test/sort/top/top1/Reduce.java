package test.sort.top.top1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
	
	private long tmp = Long.MIN_VALUE;
	
	public void reduce(LongWritable key, Iterable<NullWritable> values, Context cxt) {
		try{
			tmp = Math.max(key.get(), tmp);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		context.write(new LongWritable(tmp), NullWritable.get());
	}
	
	
	
}
