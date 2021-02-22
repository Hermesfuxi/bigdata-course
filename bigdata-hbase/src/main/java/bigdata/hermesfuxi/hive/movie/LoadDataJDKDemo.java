package bigdata.hermesfuxi.hive.movie;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * 将电影数据转换输出：java
 */
public class LoadDataJDKDemo {
    public static void main(String[] args) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\mrdata\\movie\\input\\rating.json"));

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\mrdata\\movie\\output\\out.csv"));
        String str;
        while ((str = bufferedReader.readLine()) != null){
            JSONObject jsonObject = JSONObject.parseObject(str);
            String movie = jsonObject.getString("movie");
            String timeStamp = jsonObject.getString("timeStamp");
            String rate = jsonObject.getString("rate");
            String uid = jsonObject.getString("uid");
            // 固定电影长度，rowKey 补齐
            String movieKey = StringUtils.leftPad(movie, 5, "0");
            bufferedWriter.write(movieKey + "_" + timeStamp + "," + rate + "," + uid);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedReader.close();
        bufferedWriter.close();
    }
}
