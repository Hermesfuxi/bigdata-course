package bigdata.hermesfuxi.hdfs.application.skewdemo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SkewDemo2Driver {

    private static class SkewDemo2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable intWritable = new IntWritable();
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");
            String wordKey = words[0].split("-")[0];
            if(StringUtils.isNoneBlank(wordKey)){
                text.set(wordKey);
            }
            intWritable.set(Integer.parseInt(words[1]));
            context.write(text, intWritable);
        }
    }
    private static class SkewDemo2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable valueInt = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            valueInt.set(sum);
            context.write(key, valueInt);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SkewDemo2Driver.class);

        job.setMapperClass(SkewDemo2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SkewDemo2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\skew\\output"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\skew\\output1"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0: 1);
    }
}
