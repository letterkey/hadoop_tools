package test.phoenix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Phoenix {

	public static class TMapper extends Mapper<Object, Text, Text, Text> {
		private Connection con = null;
		private PreparedStatement batchInsert = null;
		private String[] vs = null;

		private final static IntWritable one = new IntWritable(0);
		private Text v = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
			try {
//				con = DriverManager.getConnection("jdbc:phoenix:master.zkp.010251087214.tgz");
				con = DriverManager.getConnection("jdbc:phoenix:localhost");
				batchInsert = con.prepareStatement("upsert into metric_data_entity_64 (data_version, application_id, time_scope, metric_type_id, metric_id, time, "
						+ "agent_run_id, uuid, num1 , num2 , num3 , num4 , num5 , num6) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
					
				super.setup(context);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(Object key, Text value, Context context) {
			try {
				System.out.println("---------------"+value.toString());
				one.set(one.get()+1);
				vs = value.toString().split("\\t");
				System.out.println("---------------"+vs[0]);
				
				batchInsert.setInt(1, Integer.valueOf(vs[0]));
				batchInsert.setInt(2, Integer.valueOf(vs[1]));
				batchInsert.setInt(3, Integer.valueOf(vs[2]));
				batchInsert.setLong(4, Long.valueOf(vs[3]));
				batchInsert.setLong(5, Long.valueOf(vs[4]));
				batchInsert.setInt(6, Integer.valueOf(vs[5]));
				batchInsert.setInt(7, Integer.valueOf(vs[6]));
				batchInsert.setInt(8,  Integer.valueOf(vs[7]));;
				batchInsert.setFloat(9, Float.valueOf(vs[8]));
				batchInsert.setFloat(10, Float.valueOf(vs[9]));
				batchInsert.setFloat(11, Float.valueOf(vs[10]));
				batchInsert.setFloat(12, Float.valueOf(vs[11]));
				batchInsert.setFloat(13, Float.valueOf(vs[12]));
				batchInsert.setFloat(14, Float.valueOf(vs[13]));
				batchInsert.addBatch();
				if(one.get() >=3000){
					batchInsert.executeBatch();
					con.commit();
					one.set(0);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			try{
				batchInsert.executeBatch();
				con.commit();
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if (batchInsert != null) {
					try {
						batchInsert.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
			}
			super.cleanup(context);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();
		private Connection con = null;
		private PreparedStatement batchInsert = null;
		private String[] vs = null;
		private SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		public void reduce(Text key, Iterable<Text> values, Context context) {
			try {
				con = DriverManager
						.getConnection("jdbc:phoenix:dn.hdp.010104015209.tgz");
				batchInsert = con
						.prepareStatement("upsert into metric_data_entity (id,application_id,agent_run_id,metric_type_id,metric_id,"
								+ "scope_metric_id,start_time,end_time,num1,num2,num3,num4,num5,num6) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				for (Text val : values) {
					vs = val.toString().split(",");

					batchInsert.setLong(1, Long.valueOf(vs[0]));
					batchInsert.setLong(2, Long.valueOf(vs[1]));
					batchInsert.setLong(3, Long.valueOf(vs[2]));
					batchInsert.setLong(4, Long.valueOf(vs[3]));
					batchInsert.setLong(5, Long.valueOf(vs[4]));
					batchInsert.setLong(6, Long.valueOf(vs[5]));
					batchInsert.setTimestamp(7,
							new Timestamp(sdf.parse(vs[6].replaceAll("'", ""))
									.getTime()));
					batchInsert.setTimestamp(8,
							new Timestamp(sdf.parse(vs[7].replaceAll("'", ""))
									.getTime()));
					batchInsert.setFloat(9, Float.valueOf(vs[8]));
					batchInsert.setFloat(10, Float.valueOf(vs[9]));
					batchInsert.setFloat(11, Float.valueOf(vs[10]));
					batchInsert.setFloat(12, Float.valueOf(vs[11]));
					batchInsert.setFloat(13, Float.valueOf(vs[12]));
					batchInsert.setFloat(14, Float.valueOf(vs[13]));
					batchInsert.addBatch();
				}
				batchInsert.executeBatch();
				con.commit();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (batchInsert != null) {
					try {
						batchInsert.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

//		if (args.length < 2) {
//			System.err.println("Usage: wordcount <in> [<in>...] <out>");
//			System.exit(2);
//		}

		args = new String[]{"hdfs://localhost:9000/test/tpm-formatlog-metricdata.txt","hdfs://localhost:9000/out/"};
		System.out.println(args[0] + "\n" + args[1]);
		Job job = new Job(conf, "phoenixTest1");
		Path in = new Path(args[0]); // 数据输入路径
		Path out = new Path(args[1]); // 输出路径
		out.getFileSystem(conf).delete(out, true); // 删除原有输出目录

//	    
//	    File jarFile = EJob.createTempJar("bin");
//	    EJob.addClasspath("/home/hadoop/hadoop-1.2.1/conf");
//	    ClassLoader classLoader = EJob.getClassLoader();
//	    Thread.currentThread().setContextClassLoader(classLoader);
//	    ((JobConf) job.getConfiguration()).setJar(jarFile.toString()); 
	    
		job.setJarByClass(Phoenix.class);
		job.setMapperClass(TMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setOutputFormatClass(OutPutFormat.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
