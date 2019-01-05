package test.sort.secondary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinationKey implements WritableComparable<CombinationKey> {
	private static final Logger logger = LoggerFactory.getLogger(CombinationKey.class);
	
	private Text firstKey;
	private IntWritable secondKey;

	public CombinationKey() {
		this.firstKey = new Text();
		this.secondKey = new IntWritable();
	}

	@Override
	public void readFields(DataInput dateInput) throws IOException {
		this.firstKey.readFields(dateInput);
		this.secondKey.readFields(dateInput);
	}

	@Override
	public void write(DataOutput outPut) throws IOException {
		this.firstKey.write(outPut);
		this.secondKey.write(outPut);
	}

	/**
	 * 自定义比较策略 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
	 * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
	 */
	@Override
	public int compareTo(CombinationKey combinationKey) {
		logger.info("-------CombinationKey flag-------");
		
//		if (!this.getFirstKey().equals(combinationKey.getFirstKey())) {
//			return this.getFirstKey().compareTo(combinationKey.getFirstKey());
//		} else {// 按照组合键的第二个键的升序排序，将c1和c2倒过来则是按照数字的降序排序(假设2)
//			return this.getSecondKey().get() - combinationKey.getSecondKey().get();// 0,负数,正数
//		}
		return this.firstKey.compareTo(combinationKey.getFirstKey());
	}

	

	public Text getFirstKey() {
		return this.firstKey;
	}

	public void setFirstKey(Text firstKey) {
		this.firstKey = firstKey;
	}

	public IntWritable getSecondKey() {
		return this.secondKey;
	}

	public void setSecondKey(IntWritable secondKey) {
		this.secondKey = secondKey;
	}
}
