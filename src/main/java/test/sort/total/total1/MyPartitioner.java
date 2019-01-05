package test.sort.total.total1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<LongWritable, NullWritable> {

	@Override
	public int getPartition(LongWritable key, NullWritable value,
			int numPartitions) {
		long tmp = key.get();
		if (tmp <= 100) {
			return 0 % numPartitions;
		} else if (tmp <= 1000) {
			return 1 % numPartitions;
		} else {
			return 2 % numPartitions;
		}
	}

}