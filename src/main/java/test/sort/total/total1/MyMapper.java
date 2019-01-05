package test.sort.total.total1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException {

                context.write(new LongWritable(Integer.parseInt(value.toString())),
                                NullWritable.get());

        }

}