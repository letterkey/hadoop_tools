package com.oneapm.hadoop.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

/**
 * Created by lifeiyf on 2015/7/13.
 */
public class CrushFileWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(CrushFileWorker.class);
    /**
     * 要执行的job
     */
    private Job       job;
    /**
     * 要归档的path
     */
    private Path      hourPath;
    /**
     * job的output
     */
    private Path      outputPath;
    /**
     * 信号量(控制同时提交的job数)
     */
    private Semaphore semaphore;
    private CountDownLatch cdl;
    private FileSystem fs;

    public CrushFileWorker(Job job, Path hourPath, Semaphore semaphore,CountDownLatch cdl,FileSystem fs) {
        this.job = job;
        this.hourPath = hourPath;
        this.semaphore = semaphore;
        this.outputPath = new Path(hourPath.toString() + "/_output");
        this.cdl=cdl;
        this.fs=fs;
    }

    @Override public void run() {
        try {
            semaphore.acquire();
            //3、运行job
            LOG.info("......start archive ["+hourPath.toString()+"]......");
            boolean success = job.waitForCompletion(true);
            //如果job执行成功，则把数据取回到客户端指定的目录
            if (success) {
                fs = FileSystem.get(new Configuration());
                //把合并完的文件从_output目录挪回到输入目录(文件都加上ahr-前缀)
                mv(outputPath,hourPath,fs);
                //删除原始数据，即没有ahr-前缀的文件
                //5、删除原始数据(删除typepath/hourpath下的小文件，不以-big结尾或者不以all-开头的文件都删除)
                FileStatus[] stats = fs.listStatus(hourPath,new CrushFile.CrushFileFilter());
                Path[] smallFiles= FileUtil.stat2Paths(stats);
                for (Path smallFile:smallFiles){
                    fs.delete(smallFile,true);
                    LOG.info("delete "+smallFile.toString());
                }
                LOG.info("......archive success ["+hourPath.toString()+"]......");
            }else{
                LOG.info("......archive  failed ["+hourPath.toString()+"]......");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //删除hourPath下的_output目录,为失败重跑做准备
            try {
                fs.delete(outputPath,true);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                cdl.countDown();
                LOG.info("============== "+cdl.getCount()+" ==============");
                semaphore.release();
            }
        }
    }

    private void mv(Path src,Path dest,FileSystem fs){
        LOG.info("start move "+src.toString()+"---->"+dest.toString());
        try {
            FileStatus[] status = fs.listStatus(src);
            Path[] srcFiles = FileUtil.stat2Paths(status);
            for (Path srcFile:srcFiles){
                //加上all-前缀以区别大文件和小文件
                fs.rename(srcFile,new Path(dest.toString()+"/ahr-"+srcFile.getName()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
