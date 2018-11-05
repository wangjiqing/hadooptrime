package com.sakura.hadoop.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * 使用Spring Hadoop 访问HDFS
 */
public class SpringHadoopHDFSApp {

    private ApplicationContext ctx;
    private FileSystem fileSystem;

    @Before
    public void setUp() {
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
    }

    /**
     * 创建HDFS文件
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        fileSystem.mkdirs(new Path("/springdata"));
    }

    /**
     * 读取Hadoop文件内容
     * @throws IOException
     */
    @Test
    public void testText() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/springdata/hello.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    // .. 其它操作请参见 HDFSApp.java 中描述的方法

    @After
    public void tearDown() throws IOException {
        ctx = null;
        fileSystem.close();
    }
}
