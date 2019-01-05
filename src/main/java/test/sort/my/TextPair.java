package test.sort.my;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

        private String first;
        private String second;
        public TextPair(){}
		public TextPair(String f,String s){
			first = f;
			second = s;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readUTF();
			second = in.readUTF();
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(first);
			out.writeUTF(second);
		}
		
		/**
		 * 二次排序中的关键：比较函数的重载 倒叙 正序排列的算法
		 */
		@Override
		public int compareTo(TextPair o) {
			int u = first.compareTo(o.getFirst());
			int t = Integer.valueOf(second)-Integer.valueOf(o.getSecond());
			return u != 0 ? u : t;
		}

		


		public String getFirst() {
			return first;
		}

		public void setFirst(String first) {
			this.first = first;
		}

		public String getSecond() {
			return second;
		}

		public void setSecond(String second) {
			this.second = second;
		}

		@Override
		public String toString() {
			return  first + "\t" + second ;
		}
}