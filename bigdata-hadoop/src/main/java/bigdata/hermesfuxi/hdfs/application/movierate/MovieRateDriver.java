package bigdata.hermesfuxi.hdfs.application.movierate;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MovieRateDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name", "local");
        // 输出到HDFS文件系统中
//        conf.set("fs.defaultFS", "hdfs://centos01:8020");
        // 输出到本地文件系统
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("MovieRateTest");
        job.setJarByClass(MovieRateDriver.class);
        // 评论次数最多的3部电影
        getMovieRateMostCountList(job);
        // 每部电影评分最高的3条记录
//        getMovieRateHighestList(job);
        // 每部电影的评分平均值
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * 评论次数最多的3部电影
     */
    public static void getMovieRateMostCountList(Job job) throws IOException {
        job.setMapperClass(MovieRateMostCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MovieRateMostCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("H:\\movie-rate.txt"));
        FileOutputFormat.setOutputPath(job, new Path("H:\\result01"));
    }

    /**
     * 评论次数最多的3部电影 -> 可划归于最简单的计数模型，问题在于并行计算中的排序
     */
    private static class MovieRateMostCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // 评论最多： key-电影名 、 value-评论次数
        private static final Text MOVIE_NAME = new Text();
        private static final IntWritable ONE = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(StringUtils.isNoneBlank(line)){
                JSONObject obj = JSONObject.parseObject(line);
                String movie = obj.getString("movie");
                MOVIE_NAME.set(movie);
                context.write(MOVIE_NAME, ONE);
            }
        }
    }

    private static class MovieRateMostCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final Text MOVIE_NAME = new Text();
        private static final IntWritable NUM = new IntWritable();
        private static final Map<String, Integer> MAP = new ConcurrentHashMap<>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            for (IntWritable value : values) {
                sum++;
            }
            MAP.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String,Integer>> list = MAP.entrySet().stream()
                    .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()))
                    .limit(3).collect(Collectors.toList());

            for (Map.Entry<String, Integer> stringIntegerEntry : list) {
                String key = stringIntegerEntry.getKey();
                MOVIE_NAME.set(key);
                NUM.set(stringIntegerEntry.getValue());
                context.write(MOVIE_NAME, NUM);
            }
        }
    }

    /**
     * 每部电影评分最高的3条记录
     */
    public static void getMovieRateHighestList(Job job) throws IOException {
        job.setMapperClass(MovieRateHighestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MovieRateHighestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("H:\\movie-rate.txt"));
        FileOutputFormat.setOutputPath(job, new Path("H:\\result02"));
    }

    /**
     *
     */
    private static class MovieRateHighestMapper extends Mapper<LongWritable, Text, Text, Text> {
        // 评论最多： key-电影名 、 value-Rate
        private static final Text MOVIE_NAME = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(StringUtils.isNoneBlank(line)){
                JSONObject obj = JSONObject.parseObject(line);
                String movie = obj.getString("movie");
                MOVIE_NAME.set(movie);
                context.write(MOVIE_NAME, value);
            }
        }
    }

    private static class MovieRateHighestReducer extends Reducer<Text, Text, Text, Text> {
        private static final Text VALUE = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text value : values) {
                list.add(value.toString());
            }
            List<String> resultList = list.stream().sorted((entry1, entry2) -> JSONObject.parseObject(entry2).getInteger("rate").compareTo(JSONObject.parseObject(entry1).getInteger("rate"))).limit(3).collect(Collectors.toList());
            for (String str: resultList) {
                VALUE.set(str);
                context.write(key, VALUE);
            }
        }
    }
}
