package test.sort.my;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<TextPair, NullWritable,TextPair, NullWritable> {

	public void reduce(TextPair key, Iterable<NullWritable> values, Context cxt) {
		try{
			for(NullWritable v : values){
				cxt.write(key, v);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
