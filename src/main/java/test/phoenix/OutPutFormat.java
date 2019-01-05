package test.phoenix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class OutPutFormat<K, V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		System.out.println("构建recordWriter");
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
			BasicDao.conf = this.conf;

			BasicDao.push("123", value.toString().trim());
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			Log.info("execute close method");
			BasicDao.conf = this.conf;
			BasicDao.clear();
		}
	}

}
