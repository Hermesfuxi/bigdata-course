package hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Configuration 设置参数：
 *  1、在代码中 (优先级高)
 *  2、在resource目录中的 hdfs-site.xml/core-site.xml/hdfs-default.xml
 */
public class ConfigurationUtils {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("", "");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.newInstance(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileSystem != null){
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
