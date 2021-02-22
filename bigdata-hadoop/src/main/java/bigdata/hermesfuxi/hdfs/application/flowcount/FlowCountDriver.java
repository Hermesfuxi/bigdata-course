package bigdata.hermesfuxi.hdfs.application.flowcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setNumReduceTasks(2);

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\flow.log"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\result"));

        boolean b = job.waitForCompletion(true);

    }
}
