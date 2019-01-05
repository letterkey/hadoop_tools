package test.inputformat.combine;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context cxt) {
		try{
			for(Text v : values){
				cxt.write(key, v);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
