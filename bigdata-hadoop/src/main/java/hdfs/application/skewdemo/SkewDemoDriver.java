package hdfs.application.skewdemo;

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
import java.util.Random;

public class SkewDemoDriver {

    private static class SkewDemoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable one = new IntWritable(1);
        Text text = new Text();
        int numReduceTasks;
        Random random = new Random();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numReduceTasks = context.getNumReduceTasks();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                if(StringUtils.isNoneBlank(word)){
                    int randomNum = random.nextInt(numReduceTasks);
                    text.set(word + "-" + randomNum);
                    context.write(text, one);
                }
            }
        }
    }
    private static class SkewDemoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable valueInt = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum++;
            }
            valueInt.set(sum);
            context.write(key, valueInt);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SkewDemoDriver.class);

        job.setMapperClass(SkewDemoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SkewDemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\skew\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\skew\\output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0: 1);
    }
}
