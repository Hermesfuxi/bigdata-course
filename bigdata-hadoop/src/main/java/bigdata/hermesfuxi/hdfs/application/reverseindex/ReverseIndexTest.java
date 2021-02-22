package bigdata.hermesfuxi.hdfs.application.reverseindex;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Hermesfuxi
 */
public class ReverseIndexTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "ReverseIndexTest");
        job.setJarByClass(ReverseIndexTest.class);

        job.setMapperClass(ReverseIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ReverseIndexReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\index\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\index\\output3"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }

    private static class ReverseIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text textKey = new Text();
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
//            fileName = fileSplit.getPath().toString();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\s+");
            for (String str : line) {
                textKey.set(str + "-" + fileName);
                context.write(textKey, ONE);
            }
        }
    }

    private static class ReverseIndexReduce extends Reducer<Text, IntWritable, Text, Text> {
        private Map<String, String> map = new ConcurrentHashMap<String, String>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum++;
            }
            String[] split = key.toString().split("-");
            String wordKey = split[0];
            String fileName = split[1];
            String valueStr = fileName + "-" + sum;
            if (map.containsKey(wordKey)) {
                String allValue = map.get(wordKey) + ";" + valueStr;
                String sortedValues = Arrays.stream(allValue.split(";")).sorted((o1, o2) -> Integer.parseInt(o2.split("-")[1]) - Integer.parseInt(o1.split("-")[1])).collect(Collectors.joining(";"));
                map.put(wordKey, sortedValues);
            } else {
                map.put(wordKey, valueStr);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> keySet = map.keySet();
            for (String key : keySet) {
                context.write(new Text(key), new Text(map.get(key)));
            }
        }
    }
}
