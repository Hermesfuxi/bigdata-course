package hdfs.friend;

import hdfs.application.skewdemo.SkewDemoDriver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SkewDemoDriver.class);

//        job.setMapperClass();
//
//        job.setReducerClass();
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\skew\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\skew\\output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0: 1);
    }
}
