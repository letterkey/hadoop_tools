package infobright;

import infobright.util.BasicDao;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InfoBrightOutPutFormat<K, V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// Jedis jedis = RedisClient.newJedis();
		// //构建一个redis，这里你可以自己根据实际情况来构建数据库连接对象
		System.out.println("构建RedisRecordWriter");
		return new RedisRecordWriter(conf);
	}

	protected static class RedisRecordWriter<K, V> extends RecordWriter<K, V> {
		private Configuration conf;
		public RedisRecordWriter(Configuration conf) {
			this.conf = conf;
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			boolean nullKey = key == null;
			boolean nullValue = value == null;
			if (nullKey || nullValue)
				return;
			if(StringUtils.isNotEmpty(value.toString()))
				System.out.println("++++++++++++++:"+value.toString());
			BasicDao.conf = this.conf;
			BasicDao.push(value.toString().trim());
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			BasicDao.conf = this.conf;
			BasicDao.clear();
		}
	}

}
