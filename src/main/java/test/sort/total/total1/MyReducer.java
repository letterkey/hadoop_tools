package test.sort.total.total1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<NullWritable> value,
                        Context context) throws IOException, InterruptedException {

                context.write(key, NullWritable.get());

        }

}