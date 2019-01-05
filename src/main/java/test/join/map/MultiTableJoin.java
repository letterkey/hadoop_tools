package test.join.map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiTableJoin extends Configured implements Tool {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		// 用于缓存 sex、user 文件中的数据
		private Map<String, String> userMap = new HashMap<String, String>();
		private Map<String, String> sexMap = new HashMap<String, String>();

		private Text oKey = new Text();
		private Text oValue = new Text();
		private String[] kv;

		// 此方法会在map方法执行之前执行
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BufferedReader in = null;

			try {
				// 从当前作业中获取要缓存的文件
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String uidNameAddr = null;
				String sidSex = null;
				for (Path path : paths) {
					if (path.toString().contains("user")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (uidNameAddr = in.readLine())) {
							userMap.put(uidNameAddr.split("\t", -1)[0],
									uidNameAddr.split("\t", -1)[1]);
						}
					} else if (path.toString().contains("sex")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (sidSex = in.readLine())) {
							sexMap.put(sidSex.split("\t")[0],
									sidSex.split("\t")[1]);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			kv = value.toString().split("\t");
			// map join: 在map阶段过滤掉不需要的数据
			if (userMap.containsKey(kv[0]) && sexMap.containsKey(kv[1])) {
				oKey.set(userMap.get(kv[0]) + "\t" + sexMap.get(kv[1]));
				oValue.set("1");
				context.write(oKey, oValue);
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text oValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sumCount = 0;

			for (Text val : values) {
				sumCount += Integer.parseInt(val.toString());
			}
			oValue.set(String.valueOf(sumCount));
			context.write(key, oValue);
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set("df.default.name", "hdfs://master:9000/");
//		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapred.job.tracker", "master:9001");
		
		Path cache1 = new Path("hdfs://master:9000/usr/input/join/map/sex.txt");
		Path cache2 = new Path("hdfs://master:9000/usr/input/join/map/user.txt");
		Path in = new Path("hdfs://master:9000/usr/input/join/map/login.txt");
		Path out = new Path("hdfs://master:9000/usr/out/join/map");
		out.getFileSystem(conf).delete(out,true);
		
		Job job = Job.getInstance(conf, "MultiTableJoin");

		job.setJobName("MultiTableJoin");
		job.setJarByClass(MultiTableJoin.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 我们把第1、2个参数的地址作为要缓存的文件路径
		DistributedCache.addCacheFile(cache1.toUri(), job.getConfiguration());
		DistributedCache.addCacheFile(cache2.toUri(), job.getConfiguration());

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] arg = {
				"hdfs://master:9000/usr/input/join/map/user.txt",
				"hdfs://master:9000/usr/input/join/map/sex.txt"
		};
		args = arg;
		int res = ToolRunner.run(new Configuration(), new MultiTableJoin(),
				args);
		System.exit(res);
	}

}
