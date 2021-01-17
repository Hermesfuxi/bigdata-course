package hdfs.client;

import hdfs.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 *
 */
public class HDFSClient {
    private static final Logger logger = Logger.getLogger(HDFSClient.class);

    public static void main(String[] args) throws Exception {
//        logger.info("123456789");
//        mkdir("/test/");
//        deleteDir();
//        copyFromLocalFile();
//        copyToLocalFile();
//        write();
//        read();
//        rename();
//        getFileListByPath();


        // 查看文件最后修改时间
        showFileStatus();

        // 查看某个文件在HDFS集群中的元数据信息
//        listFileMetaData();
    }

    /**
     * 获取文件的元数据信息
     */
    private static void listFileMetaData() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop-master:8020"), new Configuration());
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();

            // 获取文件路径
            Path path = fileStatus.getPath();
            // 获取文件名
            String fileName = path.getName();
            // 获取上级路径
            Path parent = path.getParent();
            // 获取所在组
            String group = fileStatus.getGroup();
            // 获取所有人
            String owner = fileStatus.getOwner();
            // 获取副本个数
            short replication = fileStatus.getReplication();
            // 获取修改时间
            long modificationTime = fileStatus.getModificationTime();
            // 获取访问时间
            long accessTime = fileStatus.getAccessTime();
            // 获取权限
            FsPermission permission = fileStatus.getPermission();
            System.out.println(permission.toString()); // rw-r--r--
            System.out.println(permission.toShort()); // 权限十进制：420
            System.out.println(permission.toOctal()); // 权限八进制：644
            // 符号链接
//            Path symlink = fileStatus.getSymlink();

            // 获取长度:
            long len = fileStatus.getLen();
            // 文件逻辑切块大小
            long blockSize = fileStatus.getBlockSize() ;

            // 获取所有的数据块信息 -> 元数据
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 实际物理块长度
                long blockLocationLength = blockLocation.getLength();

                // 拓扑结构路径
                String[] topologyPaths = blockLocation.getTopologyPaths();
                System.out.println(Arrays.toString(topologyPaths)); // [/default-rack/192.168.78.130:9866, /default-rack/192.168.78.128:9866, /default-rack/192.168.78.131:9866]

                // 存在多个副本
                // 物理块的名称
                String[] names = blockLocation.getNames();
                // 物理块的主机host
                String[] hosts = blockLocation.getHosts();
                // 物理缓存的主机host
                String[] cachedHosts = blockLocation.getCachedHosts();
            }
        }
    }

    private static void showFileStatus() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(FS_DEFAULT_NAME_KEY, "hdfs://hadoop-master:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            // 逻辑位置：hdfs://hadoop-master:8020/data/test/input.csv
            Path path = fileStatus.getPath();
            System.out.println(fileStatus.getAccessTime());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getOwner());
        }
        fileSystem.close();
    }

    /**
     * 读取某个目录下的所有文件
     */
    public static void getFileListByPath() throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            long blockSize = fileStatus.getBlockSize();
            logger.info("file " + path.getName() + ": path - " + path.toString() + "  size - " + blockSize);
        }
        fileSystem.close();
    }

    /**
     * 重命名
     */
    public static void rename() throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        boolean rename = fileSystem.rename(new Path("/test3.txt"), new Path("/test/test3.txt"));
        logger.info("rename " + (rename ? "success" : "fail"));
        fileSystem.close();
    }

    /**
     * 读文件
     */
    public static void read() throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        BufferedReader bf = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("/test2.txt"))));
        String s;
        while ((s = bf.readLine()) != null) {
            logger.info(s);
        }
        bf.close();
        fileSystem.close();
    }

    /**
     * 写文件
     */
    public static void write() throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/test2.txt"));
        fsDataOutputStream.writeUTF("I dont want to go to school");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        fileSystem.close();
    }

    /**
     * 下载
     */
    public static void copyToLocalFile() throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        fileSystem.copyToLocalFile(new Path("/test.txt"), new Path("D:/test1.txt"));
        fileSystem.close();
    }

    /**
     * 上传
     */
    private static void copyFromLocalFile() throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        fileSystem.copyFromLocalFile(new Path("D:\\test.txt"), new Path("/"));
        fileSystem.close();
    }

    /**
     * 创建目录
     */
    private static void mkdir(String srcPath) throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        boolean mkdirsSuccess = fileSystem.mkdirs(new Path(srcPath));
        String info = "创建目录" + (mkdirsSuccess ? "成功" : "失败");
        logger.info(info);
        fileSystem.close();
    }

    /**
     * 删除目录
     */
    private static void deleteDir(String srcPath) throws Exception {
        FileSystem fileSystem = HDFSUtils.getHDFSFileSystem();
        Path dirPath = new Path(srcPath);
        boolean exists = fileSystem.exists(dirPath);
        if (exists) {
            boolean deleteSuccess = fileSystem.delete(dirPath, true);
            String info = "删除目录" + (deleteSuccess ? "成功" : "失败");
            logger.info(info);
        }
        fileSystem.close();
    }

    /**
     * 统计文件单词数量
     */
    public static void getFileWordCount() {
        FileSystem fileSystem = null;
        FSDataInputStream in = null;
        String strLine;
        HashMap<String, Integer> map = new HashMap<>();
        try {
            fileSystem = HDFSUtils.getHDFSFileSystem();
            in = fileSystem.open(new Path("/test.txt"));
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));//读入流并放在缓冲区
            while ((strLine = br.readLine()) != null) {
                String[] words = strLine.split("\\s+");
                for (String word : words) {
                    if (map.containsKey(word)) {
                        map.put(word, map.get(word) + 1);
                    } else {
                        map.put(word, 1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (fileSystem != null) {
                    fileSystem.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Set<String> wordSets = map.keySet();
        for (String key : wordSets) {
            System.out.println(key + "——>" + map.get(key));
        }
    }

}
