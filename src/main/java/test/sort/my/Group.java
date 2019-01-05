package test.sort.my;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * group是指在shuffle阶段的reduce端执行
 * 目标:将本reduce节点上(partition)的数据根据group中的compare规则进行分组,分组后即为reduce中的reduce函数中的数组
 * @author YMY
 */
public class Group extends WritableComparator {

	protected Group() {
		super(TextPair.class,true);
	}
	
	@Override
	public int compare(WritableComparable a,WritableComparable b){
		TextPair ak = (TextPair) a;
		TextPair bk = (TextPair) b;
		// 根据first值进行分组
		return ak.getFirst().compareTo(bk.getFirst());
	}
}
