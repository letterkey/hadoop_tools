package test.join.filter;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class ReduceBoolmFilter extends Configured implements Tool{

	/**
	 * 自定义一个输出实体
	 * **/
	private static class CombineEntity implements
			WritableComparable<CombineEntity> {
		private Text joinKey;// 连接key
		private Text flag;// 文件来源标志
		private Text secondPart;// 除了键外的其他部分的数据

		public CombineEntity() {
			// TODO Auto-generated constructor stub
			this.joinKey = new Text();
			this.flag = new Text();
			this.secondPart = new Text();
		}

		public Text getJoinKey() {
			return joinKey;
		}

		public void setJoinKey(Text joinKey) {
			this.joinKey = joinKey;
		}

		public Text getFlag() {
			return flag;
		}

		public void setFlag(Text flag) {
			this.flag = flag;
		}

		public Text getSecondPart() {
			return secondPart;
		}

		public void setSecondPart(Text secondPart) {
			this.secondPart = secondPart;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.joinKey.readFields(in);
			this.flag.readFields(in);
			this.secondPart.readFields(in);

		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.joinKey.write(out);
			this.flag.write(out);
			this.secondPart.write(out);

		}

		@Override
		public int compareTo(CombineEntity o) {
			// TODO Auto-generated method stub
			return this.joinKey.compareTo(o.joinKey);
		}
	}

	private static class JMapper extends
			Mapper<LongWritable, Text, Text, CombineEntity> {

		private CombineEntity combine = new CombineEntity();
		private Text flag = new Text();
		private Text joinKey = new Text();
		private Text secondPart = new Text();

		/**
		 * 使用布隆过滤器存储key 代替原来的HashSet存储
		 * 
		 * */
		// private HashSet&lt;String&gt; joinKeySet=new HashSet&lt;String&gt;();

		BloomFilter filter = new BloomFilter();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// 读取文件流
			BufferedReader br = null;
			String temp;
			// 获取DistributedCached里面 的共享文件
			Path paths[] = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path path : paths) {
				if (path.toString().contains("1.bin")) {
					DataInputStream strm = new DataInputStream(
							new FileInputStream(path.toString()));
					// Read into our Bloom filter.
					filter.readFields(strm);
					strm.close();
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 获得文件输入路径
			String pathName = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			if (pathName.endsWith("2.txt")) {
				String valueItems[] = value.toString().split(",");
				/**
				 * 在这里过滤必须要的连接字符
				 * */
				// if(joinKeySet.contains(valueItems[0])){
				if (filter.membershipTest(new Key(valueItems[0].getBytes()))) {
					// 设置标志位
					flag.set("0");
					// 设置链接键
					joinKey.set(valueItems[0]);
					// 设置第二部分
					secondPart.set(valueItems[1] + "\t" + valueItems[2]);

					// 封装实体
					combine.setFlag(flag);// 标志位
					combine.setJoinKey(joinKey);// 链接键
					combine.setSecondPart(secondPart);// 其他部分

					// 写出
					context.write(combine.getJoinKey(), combine);
				} else {
					System.out.println("a.txt里");
					System.out.println("在小表中无此记录，执行过滤掉！");
					for (String v : valueItems) {
						System.out.print(v + "   ");
					}
					return;
				}

			} else if (pathName.endsWith("b.txt")) {
				String valueItems[] = value.toString().split(",");
				/**
				 * 判断是否在集合中
				 * */
				if (filter.membershipTest(new Key(valueItems[0].getBytes()))) {
					// if(joinKeySet.contains(valueItems[0])){
					// if(joinKeySet.contains(valueItems[0])){
					// 设置标志位
					flag.set("1");

					// 设置链接键
					joinKey.set(valueItems[0]);

					// 设置第二部分注意不同的文件的列数不一样
					secondPart.set(valueItems[1] + "\t" + valueItems[2] + "\t"
							+ valueItems[3]);

					// 封装实体
					combine.setFlag(flag);// 标志位
					combine.setJoinKey(joinKey);// 链接键
					combine.setSecondPart(secondPart);// 其他部分

					// 写出
					context.write(combine.getJoinKey(), combine);
				} else {
					// 执行过滤 ......
					System.out.println("b.txt里");
					System.out.println("在小表中无此记录，执行过滤掉！");
					for (String v : valueItems) {
						System.out.print(v + "   ");
					}
					return;
				}
			}
		}
	}

	private static class JReduce extends
			Reducer<Text, CombineEntity, Text, Text> {
		// 存储一个分组中左表信息
		private List<Text> leftTable = new ArrayList<Text>();
		// 存储一个分组中右表信息
		private List<Text> rightTable = new ArrayList<Text>();

		private Text secondPart = null;

		private Text output = new Text();

		// 一个分组调用一次
		@Override
		protected void reduce(Text key, Iterable<CombineEntity> values,
				Context context) throws IOException, InterruptedException {
			leftTable.clear();// 清空分组数据
			rightTable.clear();// 清空分组数据
			/**
			 * 将不同文件的数据，分别放在不同的集合 中，注意数据量过大时，会出现 OOM的异常
			 * **/
			for (CombineEntity ce : values) {
				this.secondPart = new Text(ce.getSecondPart().toString());
				// 左表
				if (ce.getFlag().toString().trim().equals("0")) {
					leftTable.add(secondPart);

				} else if (ce.getFlag().toString().trim().equals("1")) {

					rightTable.add(secondPart);
				}
			}
			// =====================
			for (Text left : leftTable) {
				for (Text right : rightTable) {
					output.set(left + "\t" + right);// 连接左右数据
					context.write(key, output);// 输出
				}
			}
		}
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String[] arg = {
				"hdfs://master:9000/usr/input/bloom/2.txt",
				"hdfs://master:9000/usr/output" 
		};
		args = arg;
		ToolRunner.run(new Configuration(), new ReduceBoolmFilter(),args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if(arg0.length!=2){
			System.err.println("Usage:\nfz.outputformat.FileOutputFormatDriver <in> <out> <numReducer>");
			return -1;
		}
		Configuration conf = getConf();
		
		conf.set("df.default.name", "hdfs://master:9000/");
//		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapred.job.tracker", "master:9001");
//		conf.set("mapred.min.split.size", "64");
		
		Path in = new Path(arg0[0]);
		Path out= new Path(arg0[1]);
		
		// 删除已经存在的输出目录
		boolean delete=out.getFileSystem(conf).delete(out, true);
		System.out.println("deleted "+out+"?"+delete);

		// 小表共享
		String bpath = "hdfs://master:9000/usr/input/bloom/1.bin";
		// 添加到共享cache里
		DistributedCache.addCacheFile(new URI(bpath), conf);

		Job job = Job.getInstance(conf,"bloomfilter");
		job.setJarByClass(ReduceBoolmFilter.class);
		System.out.println("模式：  " + conf.get("mapred.job.tracker"));
		
		// 设置Map和Reduce自定义类
		job.setMapperClass(JMapper.class);
		job.setReducerClass(JReduce.class);

		// 设置Map端输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CombineEntity.class);

		// 设置Reduce端的输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}