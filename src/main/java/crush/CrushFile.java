package com.oneapm.hadoop.job;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

/**
 * Created by lifeiyf on 2015/7/13.
 */
public class CrushFile {
    private static final Log    LOG  = LogFactory.getLog(CrushFile.class);
    public final static  String NAME = "crushfile";
    /**
     * 输入目录(到日期)
     */
    private static Path   input;
    /**
     * 指定topic
     */
    private static String topic;
    /**
     * 每个reduce任务给多少内存，默认1024mb
     */
    private static int memPerReduce  = 1024;
    /**
     * 每个reduce处理多大的数据，默认1024mb
     */
    private static int sizePerReduce = 1024;
    private static Semaphore     semaphore;
    private static String        compressionCodec;
    private static Configuration conf;
    private static FileSystem    fs;
    static Map<String, Class<? extends CompressionCodec>> compressionTypes = new HashMap<String, Class<? extends CompressionCodec>>();

    static {
        compressionTypes.put("snappy", org.apache.hadoop.io.compress.SnappyCodec.class);
        compressionTypes.put("gz", org.apache.hadoop.io.compress.GzipCodec.class);
        compressionTypes.put("lz4", org.apache.hadoop.io.compress.Lz4Codec.class);
        conf = new Configuration();
        //默认中间结果压缩
        conf.setBoolean("mapreduce.map.output.compress",true);
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            System.err.println("can not create FileSystem");
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public static class CrushFileMapper extends Mapper<LongWritable, BytesWritable, Text, BytesWritable> {
        private Text outKey = new Text();
        private Random random=new Random();

        public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            outKey.set(random.nextInt()+"");
            context.write(outKey, value);
        }
    }


    static class CrushFileReducer extends Reducer<Text, BytesWritable, NullWritable, BytesWritable> {

        @SuppressWarnings({"unchecked", "rawtypes"}) @Override protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
                throws IOException, InterruptedException {
            for (BytesWritable value : values) {
                context.write(NullWritable.get(),value);
            }
        }
    }

    public static Job createSubmittableJob(Configuration conf, Path hourPath) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJobName("archive-[" + hourPath.toString() + "]");
        job.setJarByClass(CrushFile.class);

        //set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(CombineSequenceFileInputFormat.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        //MultipleOutputs.addNamedOutput(job, "all", SequenceFileOutputFormat.class, NullWritable.class, BytesWritable.class);
        // the keys are words (strings)
        job.setOutputKeyClass(NullWritable.class);
        // the values are counts (ints)
        job.setOutputValueClass(BytesWritable.class);

        //use the defined mapper
        job.setMapperClass(CrushFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setReducerClass(CrushFileReducer.class);

        //不允许递归
        FileInputFormat.setInputDirRecursive(job, false);
        FileInputFormat.addInputPath(job, hourPath);

        FileOutputFormat.setOutputPath(job, new Path(hourPath.toString() + "/_output"));
        //压缩
        if (null != compressionCodec) {
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            FileOutputFormat.setOutputCompressorClass(job, compressionTypes.get(compressionCodec));
        }
        //计算输入目录的大小决定reduce个数
        job.setNumReduceTasks(getNumReduceTasks(hourPath));
        return job;
    }

    private static int getNumReduceTasks(Path hourPath) {
        ContentSummary cos= null;
        try {
            cos = fs.getContentSummary(hourPath);
            long size=cos.getLength()/1024/1024;
            int numReduce=Integer.parseInt(size/sizePerReduce+"")+1;
            LOG.info("*********"+hourPath.toString()+"["+size+"mb] ,sizePerReduce="+sizePerReduce+",numReduce="+numReduce+"******");
            return numReduce;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        try{
            //1、参数验证
            if (args.length < 2) {
                printUsage();
            }

            // 这一步之后命令行中的-D参数项就不在otherArgs中了，已经被设置到conf了
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            paramCheck(otherArgs);

            //如果没有显示指定reduce内存(-Dmapreduce.reduce.memory.mb=xxx)，则根据比例给定一个经验值
            conf.set("mapreduce.reduce.memory.mb",memPerReduce+"");
            //如果指定了topic，则直接对该topic下的目录进行归档
            if(StringUtils.isNotEmpty(topic)){
                topicArchive(new Path(input.toString()+"/"+topic));
            }else{//如果未指定topic，则找出所有topic下的小时目录，统一调度
                //获取要压缩的小时目录
                Path[] paths=getJobList();
                //job调度
                int length=paths.length;
                LOG.info("============== "+length+" directories need to archive =============");
                if(null!=paths&&length>0){
                    CountDownLatch cdl = new CountDownLatch(length);
                    for (Path hourPath : paths) {
                        Job job = createSubmittableJob(conf, hourPath);
                        if(null!=job){
                            new CrushFileWorker(job, hourPath, semaphore, cdl, fs).start();
                        }else{
                            cdl.countDown();
                        }
                    }
                    cdl.await();
                    LOG.info("============== All job runs successfully ==============");
                }else{
                    System.err.println("============== no topic need to archive ==============");
                    System.exit(0);
                }
            }
        }finally {
            if (null != fs)
                fs.close();
        }
    }

    /**
     * 归档某个topic
     * @param topicPath
     */
    public static void topicArchive(Path topicPath) {
        String topicName=topicPath.getName();
        LOG.info("--------start archive ["+topicName+"]--------");
        try {
            FileStatus[] status = fs.listStatus(topicPath);
            Path[] hourPaths = FileUtil.stat2Paths(status);
            int leng = hourPaths.length;
            if (null != hourPaths && leng > 0) {
                CountDownLatch cdl = new CountDownLatch(leng);
                for (Path hourPath : hourPaths) {
                    Job job = createSubmittableJob(conf, hourPath);
                    if(null!=job){
                        new CrushFileWorker(job, hourPath, semaphore, cdl, fs).start();
                    }else{
                        cdl.countDown();
                    }
                }
                cdl.await();
                LOG.info("--------end archive ["+topicName+"]--------");
            } else {
                System.err.println("["+topicName+"] no file need to crush ");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Path[] getJobList() {
        List<Job> jobList=new ArrayList<Job>();
        Path dir=new Path(input.toString()+"/*/*");
        try{
            FileStatus[] stats = fs.globStatus(dir, new PathFilter() {
                @Override public boolean accept(Path path) {
                    try {
                        return !fs.exists(new Path(path.toString() + "/ahr-_SUCCESS"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return true;
                }
            });
            //Path[] subPaths= FileUtil.stat2Paths(stats);
            return FileUtil.stat2Paths(stats);
            /**
            for (Path path:subPaths){
                jobList.add(createSubmittableJob(conf, path));
            }
             */
        }catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    private static void printUsage() {
        System.out.println("Usage : crushfile -i <input_dir> -s <semaphore> [-t <topic>] [-mem <memPerReduce>] [-size <sizePerReduce>] [-c <compressionCodec>] ");
    }

    public static void paramCheck(String[] args) throws ParseException {
        // 参数验证
        Options options = new Options();

        options.addOption("i", "input", true, "input");
        options.addOption("s", "semaphore", true, "semaphore");
        options.addOption("t", "topic", true, "topic");
        options.addOption("mem", "memPerReduce", true, "memPerReduce");
        options.addOption("size", "sizePerReduce", true, "sizePerReduce");
        options.addOption("c", "compressionCodec", true, "compressionCodec");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        String parser_error = "ERROR while parsing arguments. ";
        if (cmd.hasOption("i")) {
            String inputPath = cmd.getOptionValue("i");
            input = new Path(inputPath);
            LOG.info("inputPath = " + inputPath);
        } else {
            System.err.println(parser_error + "Please provide input");
            System.exit(0);
        }

        if (cmd.hasOption("t")) {
            topic = cmd.getOptionValue("t");
            LOG.info("topic = " + topic);
        }

        if (cmd.hasOption("mem")) {
            memPerReduce = Integer.parseInt(cmd.getOptionValue("mem"));
            if (memPerReduce <= 0) {
                System.err.println(parser_error + "memPerReduce must be greater than zero");
                System.exit(0);
            }
            LOG.info("memPerReduce = " + memPerReduce);
        }
        if (cmd.hasOption("size")) {
            sizePerReduce = Integer.parseInt(cmd.getOptionValue("size"));
            if (sizePerReduce <= 0) {
                System.err.println(parser_error + "sizePerReduce must be greater than zero");
                System.exit(0);
            }
            LOG.info("sizePerReduce = " + sizePerReduce);
        }

        if (cmd.hasOption("s")) {
            int sem = Integer.parseInt(cmd.getOptionValue("s"));
            if (sem <= 0) {
                System.err.println(parser_error + "semaphore must be greater than zero");
                System.exit(0);
            } else {
                semaphore = new Semaphore(sem);
                LOG.info("semaphore = " + sem);
            }
        } else {
            System.err.println(parser_error + "Please provide semaphore");
            System.exit(0);
        }

        if (cmd.hasOption("c")) {
            compressionCodec = cmd.getOptionValue("c");
            if (!compressionTypes.containsKey(compressionCodec)) {
                System.err.println(parser_error + "compressionCodec must be [" + StringUtils.join(compressionTypes.keySet(), "/") + "]");
                System.exit(0);
            }
        }
        LOG.info("compressionCodec = " + compressionCodec);
    }

    /**
     * 过滤出被合并的文件(不以all-开头的文件)
     */
    public static class CrushFileFilter implements PathFilter {
        @Override public boolean accept(Path path) {
            String pathName = path.getName();
            if (pathName.startsWith("ahr-")) {
                return false;
            }
            return true;
        }
    }
}
