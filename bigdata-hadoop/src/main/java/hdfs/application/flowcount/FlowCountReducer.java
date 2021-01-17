package hdfs.application.flowcount;

import hdfs.utils.LoggerUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class FlowCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("up", 0);
        map.put("down", 0);
        for (Text value : values) {
            String str = value.toString();
            LoggerUtils.info(str);
            String[] strings = str.split("-");
            String type = strings[0];
            int parseInt = Integer.parseInt(strings[1]);
            if ("up".equals(type)) {
                map.put("up", map.get("up") + parseInt);
            } else if ("down".equals(type)) {
                map.put("down", map.get("down") + parseInt);
            }
        }
        context.write(key, new Text("upFlow:"+ map.get("up") + " "+ "downFlow:"+ map.get("down")));
    }
}
