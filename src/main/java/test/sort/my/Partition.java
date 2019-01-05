package test.sort.my;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * partition在shuffle中的map阶段执行
 * 目标:对每条数据进行标记(贴标签)该有哪个reduce处理
 * @author YMY
 *
 */
public class Partition extends Partitioner<TextPair, NullWritable> {
	public static Logger log = LoggerFactory.getLogger(Partition.class);
	
	/**
	 * 分片，将相同的key换分到一个partition中
	 */
	@Override
	public int getPartition(TextPair key, NullWritable value, int numPartition) {
		// 返回本记录的partition编码
		int p= (key.getFirst().hashCode()) & Integer.MAX_VALUE % numPartition;
		log.info("partition："+key.getFirst()+":"+key.getSecond()+":"+p);
		return p;
	}

}
