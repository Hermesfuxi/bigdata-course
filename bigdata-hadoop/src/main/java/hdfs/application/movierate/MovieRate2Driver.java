package hdfs.application.movierate;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MovieRate2Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name", "local");
        // 输出到HDFS文件系统中
//        conf.set("fs.defaultFS", "hdfs://centos01:8020");
        // 输出到本地文件系统
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("MovieRateTest");
        job.setJarByClass(MovieRate2Driver.class);
        // 评论次数最多的3部电影
        getMovieRateMostCountList(job);
        // 每部电影的评分平均值
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * 评论次数最多的3部电影
     */
    public static void getMovieRateMostCountList(Job job) throws IOException {
        job.setMapperClass(MovieRateMostCountMapper.class);
        job.setMapOutputKeyClass(MovieRateBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setPartitionerClass(MovieRatePartitioner.class);

        job.setReducerClass(MovieRateMostCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("H:\\movie-rate.txt"));
        FileOutputFormat.setOutputPath(job, new Path("H:\\result02"));
    }

    /**
     * 评论次数最多的3部电影 -> 可划归于最简单的计数模型，问题在于并行计算中的排序
     */
    private static class MovieRateMostCountMapper extends Mapper<LongWritable, Text, MovieRateBean, NullWritable> {
        // 评论最多： key-电影名 、 value-评论次数
        private static final Text MOVIE_NAME = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(StringUtils.isNoneBlank(line)){
                MovieRateBean movieRateBean = JSONObject.parseObject(line, MovieRateBean.class);
                context.write(movieRateBean, NullWritable.get());
            }
        }
    }

    private static class MovieRatePartitioner extends Partitioner<MovieRateBean, NullWritable> {
        @Override
        public int getPartition(MovieRateBean movieRateBean, NullWritable nullWritable, int numPartitions) {
            return (movieRateBean.getMovie().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    private static class MovieRateMostCountReducer extends Reducer<MovieRateBean, NullWritable, Text, IntWritable> {
        private static final Text MOVIE_NAME = new Text();
        private static final IntWritable NUM = new IntWritable();
        private static final Map<String, Integer> MAP = new ConcurrentHashMap<>();

        @Override
        protected void reduce(MovieRateBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        }
    }

}
