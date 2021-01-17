package hdfs.application.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setNumReduceTasks(2);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入
        FileInputFormat.setInputPaths(job, new Path("D://tmp/test.txt"));

        // 输出
        FileOutputFormat.setOutputPath(job, new Path("D://tmp/result"));;

        boolean b = job.waitForCompletion(true);
    }
}
