package com.hermesfuxi.demo.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.hermesfuxi.demo.interceptor.utils.MD5Utils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author hermesfuxi
 */
public class EventFieldEncryptInterceptor implements Interceptor {
    private ParseTypeProcessor parseTypeProcessor;

    public EventFieldEncryptInterceptor(ParseTypeProcessor parseTypeProcessor) {
        this.parseTypeProcessor = parseTypeProcessor;
    }

    /**
     * 初始化方法，在正式调用拦截逻辑之前，会先调用一次
     */
    @Override
    public void initialize() {
        System.out.println("EventFieldEncryptInterceptor start");
    }

    /**
     * 拦截的处理逻辑所在方法
     *
     * 假设，我们要采集的数据，格式如下：
     * id,name,timestamp,devicetype,event
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        String line = new String(body);

        String resultValue = "";

        // 从事件内容中提取所需数据
        if("String".equals(parseTypeProcessor.getParseType())){
            String[] split = line.split(parseTypeProcessor.getSplitChar());
            Integer index = parseTypeProcessor.getIndex();
            String value = split[index];
            split[index] = MD5Utils.encrypt(value);
            resultValue = String.join(parseTypeProcessor.getSplitChar(), split);

        }else if("json".equals(parseTypeProcessor.getParseType())) {
            String indexKey = parseTypeProcessor.getIndexKey();
            JSONObject jsonObject = JSONObject.parseObject(line);
            String value = jsonObject.getString(indexKey);
            jsonObject.put(indexKey, MD5Utils.encrypt(value));
            resultValue = jsonObject.toJSONString();
        }

        // 将数据放入body
        event.setBody(resultValue.getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }


    /**
     * 关闭清理方法，在销毁该拦截器实例之前，会调用一次
     */
    @Override
    public void close() {
        System.out.println("EventFieldEncryptInterceptor close");
    }


    /**
     * builder是用于提供给flume来构建自定义拦截器对象的
     *
     */
    public static class EventFieldEncryptInterceptorBuilder implements Builder{
        ParseTypeProcessor parseTypeProcessor;
        /**
         * flume会调用该方法来创建我们的自定义拦截器对象
         * @return
         */
        @Override
        public EventFieldEncryptInterceptor build() {
            return new EventFieldEncryptInterceptor(parseTypeProcessor);
        }

        /**
         * flume会将加载的参数，通过该方法传递进来
         */
        @Override
        public void configure(Context context) {
            parseTypeProcessor = new ParseTypeProcessor(context);
        }
    }

    private static class ParseTypeProcessor {
        // 解析后生成的 headerName 值
        private String headerName;

        // 解析方式
        private String parseType;

        // 解析方式 为 String: csv/tsv
        private String splitChar;
        private Integer index;

        // 解析方式 为 Json
        private String indexKey;

        public ParseTypeProcessor(Context context) {
            this.headerName = context.getString("headerName", "timestamp");
            this.setHeaderName(headerName);

            String type = context.getString("parseType", "string");
            Integer index = context.getInteger("index", 0);
            String indexKey = context.getString("indexKey", "timestamp");

            if("csv".equals(parseType)){
                this.setSplitChar(",");
                type = "string";
            }else if("tsv".equals(parseType)){
                type = "string";
                this.setSplitChar("[\\s\t]+");
            }
            this.parseType = type;
            this.setIndex(index);
            this.setIndexKey(indexKey);
        }

        public String getHeaderName() {
            return headerName;
        }

        public void setHeaderName(String headerName) {
            this.headerName = headerName;
        }

        public String getParseType() {
            return parseType;
        }

        public void setParseType(String parseType) {
            this.parseType = parseType;
        }

        public String getSplitChar() {
            return splitChar;
        }

        public void setSplitChar(String splitChar) {
            this.splitChar = splitChar;
        }

        public Integer getIndex() {
            return index;
        }

        public void setIndex(Integer index) {
            this.index = index;
        }

        public String getIndexKey() {
            return indexKey;
        }

        public void setIndexKey(String indexKey) {
            this.indexKey = indexKey;
        }
    }
}
