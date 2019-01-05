package infobright;

import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner<K1,V1> extends Partitioner<K1,V1> {

	@Override
	public int getPartition(K1 key, V1 value, int numPartitions) {
		CustomKey keyK= (CustomKey) key;
		return (keyK.getTimeStamp().hashCode() & Integer.MAX_VALUE)%numPartitions;
	}

}
