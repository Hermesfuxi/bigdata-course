package hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

//import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class HDFSUtils {
    // TODO 单例模式

    /**
     *
     * @return 文件系统
     * @throws Exception IOException
     */
    public static FileSystem getHDFSFileSystem() throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(FS_DEFAULT_NAME_KEY, "hdfs://hadoop-master:8020");
//        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop-master:8020"), configuration);
        return FileSystem.newInstance(new URI("hdfs://hadoop-master:8020"), configuration);
    }

}
