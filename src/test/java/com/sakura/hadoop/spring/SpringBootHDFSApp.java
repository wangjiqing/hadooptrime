package com.sakura.hadoop.spring;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

import java.util.Collection;

/**
 * 使用Spring Boot的方式访问HDFS
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {

    @Autowired
    FsShell fsShell;

    public void run(String... strings) {
        Collection<FileStatus> fileStatuses = fsShell.lsr("/springdata");
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("> " + fileStatus.getPath().toString());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHDFSApp.class, args);
    }
}
