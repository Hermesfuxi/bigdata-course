package hdfs.application.userorder;


import hdfs.application.userorder.domain.UserOrderBean;
import hdfs.utils.LoggerUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.cglib.beans.BeanCopier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class UserOrderDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(UserOrderDriver.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(UserOrderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserOrderBean.class);

        job.setReducerClass(UserOrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\join\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\DOIT19-DAY06-HDP03\\mrdata\\join\\output4"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    private static class UserOrderMapper extends Mapper<LongWritable, Text, Text, UserOrderBean> {
        private String fileName;
        private final Text text = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(",");
            String uidKey;
            UserOrderBean userOrderBean;
            // 通过文件名区分用户信息与订单信息，并将用户信息Bean 的 orderId 设置为“userInfo”
            if (fileName.startsWith("user") && strings.length >= 5) {
                uidKey = strings[0];
                userOrderBean = new UserOrderBean("userInfo", strings[0], strings[1], Integer.parseInt(strings[2]), strings[3], strings[4]);
            } else if (strings.length >= 2) {
                uidKey = strings[1];
                // 订单信息中没有的属性，全部设置默认值（序列化与反序列化需要）
                userOrderBean = new UserOrderBean(strings[0], strings[1], "", 0, "", "");
            } else {
                LoggerUtils.error("数据或系统解析错误, data: " + value.toString());
                return;
            }
            // 使用 uid 为 key， 这样才能让组合
            text.set(uidKey);
            context.write(text, userOrderBean);
        }
    }

    private static class UserOrderReducer extends Reducer<Text, UserOrderBean, UserOrderBean, NullWritable> {
        private static final BeanCopier BEAN_COPIER = BeanCopier.create(UserOrderBean.class, UserOrderBean.class, false);

        @Override
        protected void reduce(Text key, Iterable<UserOrderBean> values, Context context) throws IOException, InterruptedException {
            List<UserOrderBean> userOrderBeanList = new ArrayList<>();
            String userName = null;
            Integer age = null;
            String sex = null;
            String dishName = null;
            for (UserOrderBean value : values) {
                // 相同的UID下，应该将 userInfo ——> orderInfo
                if ("userInfo".equals(value.getOrderId())) {
                    userName = value.getUserName();
                    age = value.getAge();
                    sex = value.getSex();
                    dishName = value.getDishName();
                } else {
                    // 使用Bean的深copy
                    UserOrderBean orderInfoBean = new UserOrderBean();
                    BEAN_COPIER.copy(value, orderInfoBean, null);
                    userOrderBeanList.add(orderInfoBean);
                }
            }
            if(StringUtils.isBlank(userName)){
                LoggerUtils.error("数据有误!!!!!!!!!!");
            }
            // 排序后直接输出
            userOrderBeanList.sort(Comparator.comparing(UserOrderBean::getUid));
            for (UserOrderBean userOrderBean : userOrderBeanList) {
                userOrderBean.setUserName(userName);
                userOrderBean.setAge(age);
                userOrderBean.setSex(sex);
                userOrderBean.setDishName(dishName);
                context.write(userOrderBean, NullWritable.get());
            }

        }
    }
}
