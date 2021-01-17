package hdfs.application.wordcount;

import hdfs.utils.LoggerUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 每行执行一次
     * @param key 起初偏移量
     * @param value 读取的行的内容
     * @param context  操作的内容
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LoggerUtils.info("Mapper: " + key + " ——> " + value);
        if(value != null){
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
