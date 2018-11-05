package com.sakura.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    public static final String HDFS_USER = "hadoop";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
    }

    /**
     * 创建一个HDFS目录
     *
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 向HDFS文件系统中创建一个文件
     * 注释：这里测试的hdfs服务器需要打开 50010 端口进行通信
     * 详见 https://stackoverflow.com/questions/14288453/writing-to-hdfs-from-java-getting-could-only-be-replicated-to-0-nodes-instead
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        outputStream.write("Hello Hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 读取HDFS文件系统中一个指定路径下的文件中的内容
     * 注释： 需要打开core-site.xml 文件中配置的端口 默认配置是 9000 这里配置 8020
     * @throws Exception
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名一个文件
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/ab.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 小文件从本地拷贝到hdfs文件系统
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("E:/CorsFilter.java");
        Path hdfsPath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传一个大文件到hdfs系统，并以 ... 做等待效果
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:/GitHubDesktopSetup.exe")));
        FSDataOutputStream fos = fileSystem.create(new Path("/bigfile/GitHubDesktopSetup"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, fos, 4096);
    }

    /**
     * 从hdfs文件服务器上下载一个文件到本地
     * delSrc 是否删除本地临时文件src
     * userRawLocalFileSystem 是否使用RoLoopLoFielScript作为本地文件系统
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("E:/");
        Path hdfsPath = new Path("/hdfsapi/test/ab.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

    /**
     * 列出指定目录下的文件或目录（不包含子文件）
     * 注释： 使用hdfs shell 的方式puts文件默认采用hdfs-site.xml配置的副本系数
     * JavaAPI调用的时候采用Hadoop自己的副本系数
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test/"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件"; // 类型
            short replication = fileStatus.getReplication();    // 副本系数
            long len = fileStatus.getLen();                     // 文件大小
            String path = fileStatus.getPath().toString();      // 文件路径
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 递归删除一个目录下的所有文件
     * @throws Exception
     */
    @Test
    public void deleteFiles() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/"), true);
    }

    @After
    public void tearDown() throws Exception {
        configuration.clear();
        fileSystem.close();

        configuration = null;
        fileSystem = null;

        System.out.println("HDFSApp.tearDown");
    }
}
