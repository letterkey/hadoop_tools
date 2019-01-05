package test.join.filter;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class BloomFilteringDriver {

	public static class BloomFilteringMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private BloomFilter filter = new BloomFilter();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BufferedReader in = null;
			try {
				// 从当前作业中获取要缓存的文件
				Path[] paths = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());
				for (Path path : paths) {
					if (path.toString().contains("bloom.bin")) {
						DataInputStream strm = new DataInputStream(
								new FileInputStream(path.toString()));
						// Read into our Bloom filter.
						filter.readFields(strm);
						strm.close();
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

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get the value for the comment
			String comment = value.toString();
			// If it is null, skip this record
			if (comment == null || comment.isEmpty()) {
				return;
			}
			StringTokenizer tokenizer = new StringTokenizer(comment);
			// For each word in the comment
			while (tokenizer.hasMoreTokens()) {
				// Clean up the words
				String cleanWord = tokenizer.nextToken().replaceAll("'", "").replaceAll("[^a-zA-Z]", " ");
				// If the word is in the filter, output it and break
				if (cleanWord.length() > 0 && filter.membershipTest(new Key(cleanWord.getBytes()))) {
					context.write(new Text(cleanWord), NullWritable.get());
					// break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set("df.default.name", "hdfs://master:9000/");
//		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapred.job.tracker", "master:9001");
		
		Path cache = new Path("hdfs://master:9000/usr/input/bloom/bloom.bin");
		Path input = new Path("hdfs://master:9000/usr/input/bloom/user2.txt");
		Path out = new Path("hdfs://master:9000/usr/out/bloom");
		
		FileSystem.get(conf).delete(out, true);
		Job job = new Job(conf, "TestBloomFiltering");
		job.setJarByClass(BloomFilteringDriver.class);
		job.setMapperClass(BloomFilteringMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, out);
		DistributedCache.addCacheFile(new Path("/usr/input/bloom/bloom.bin").toUri(),
				job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
