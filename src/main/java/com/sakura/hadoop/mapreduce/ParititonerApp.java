package com.sakura.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 指定某一个job在某个分区
 */
public class ParititonerApp {

    /**
     * Map: 读取输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        public static LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 接收到的每行数据
            String lines = value.toString();
            // 通过制定分隔符拆分
            String[] words = lines.split(" ");

            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
        }
    }

    /**
     * Reduce：归并操作
     */
    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                // 求key出现的次数总和
                sum += value.get();
            }
            // 输出
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * paritioner
     */
    public static class MyPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {

            if (key.toString().equals("xiaomi")) {
                return 0;
            } else if (key.toString().equals("huawei")) {
                return 1;
            } else if (key.toString().equals("iphone7")) {
                return 2;
            }
            return 3;
        }
    }

    /**
     * 定义启动Driver：封装Map Reduce作业的所有信息
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 创建Configuration
        Configuration configuration = new Configuration();

        // 清理已经存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, is has be deleted!");
        }

        // 创建job
        Job job = Job.getInstance(configuration, "wordcount");
        // 设置job的处理类
        job.setJarByClass(ParititonerApp.class);
        // 设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置Map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置Reduce相关参数
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置job的partition
        job.setPartitionerClass(MyPartitioner.class);
        // 设置4个reducer，每个分区一个
        job.setNumReduceTasks(4);

        // 设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
