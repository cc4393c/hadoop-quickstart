package com.mashibing.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    // hadoop框架中，数据会做序列化/反序列化
    // hadoop可以序列化/反序列化类型，或者自己开发类型
    // 自己开发的类型必须实现序列化/反序列化接口和比较器接口
    // 排序 -> 比较 这个世界上有两种顺序是基本的（数值序、字典序）

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // hello hadoop 1
    // hello hadoop 2
    // TextInputFormat
    // key 是每一行字符串第一个字节面向文件的偏移量
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
