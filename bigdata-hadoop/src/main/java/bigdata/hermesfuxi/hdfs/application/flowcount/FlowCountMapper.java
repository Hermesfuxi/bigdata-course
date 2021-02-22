package bigdata.hermesfuxi.hdfs.application.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Hermesfuxi-PC
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split("\\s+");
        if (strings.length > 6) {
            String tel = strings[1];

            String downFlow = strings[strings.length - 2];
            context.write(new Text(tel), new Text("down-" + downFlow));

            String upFlow = strings[strings.length - 3];
            context.write(new Text(tel), new Text("up-" + upFlow));
        }
    }
}
