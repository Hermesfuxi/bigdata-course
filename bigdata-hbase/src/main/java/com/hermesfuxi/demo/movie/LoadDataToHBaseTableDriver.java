package com.hermesfuxi.demo.movie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author hermesfuxi
 * 将电影数据传到 hdfs，并使用 借助 MR、hbase 排序
 * 1、读取数据并处理
 * 2、输出到HBase
 */
public class LoadDataToHBaseTableDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-master2,hadoop-slave1,hadoop-slave2,hadoop-slave3");
        Job job = Job.getInstance(conf, "load movie data to hbase table");
        job.setJarByClass(LoadDataToHBaseTableDriver.class);

//        job.setMapperClass();
//        job.setMapOutputKeyClass();
//        job.setMapOutputValueClass();
//
//        job.setReducerClass();
//        job.setOutputKeyClass();
//        job.setOutputValueClass();

        FileInputFormat.setInputPaths(job, new Path(""));
//        TableMapReduceUtil.initTableReducerJob();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
