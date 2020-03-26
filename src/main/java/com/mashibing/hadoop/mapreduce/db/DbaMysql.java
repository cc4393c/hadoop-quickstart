package com.mashibing.hadoop.mapreduce.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;

import java.io.IOException;
import java.util.Iterator;

public class DbaMysql {
    public static class DBAccessMapper extends MapReduceBase
            implements Mapper<LongWritable, StudentRecord, IntWritable, Text> {
        @Override
        public void map(LongWritable key, StudentRecord value, OutputCollector<IntWritable, Text> output,
                        Reporter reporter) throws IOException {
            output.collect(new IntWritable(value.getId()), new Text(value.toString()));
        }
    }

    public static class DBAccessReducer extends MapReduceBase
            implements Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterator<Text> values,
                           OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // windows平台运行
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        // 本地运行
        configuration.set("mapreduce.framework.name", "local");

        JobConf jobConf = new JobConf(configuration);

        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setInputFormat(DBInputFormat.class);

        String[] fields = {"id", "name"};
        DBInputFormat.setInput(jobConf,
                StudentRecord.class, "user", "length(name)>2", "", fields);
        DBConfiguration.configureDB(jobConf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test", "root", "123456");

        jobConf.setMapperClass(DBAccessMapper.class);
        jobConf.setReducerClass(DBAccessReducer.class);

        FileOutputFormat.setOutputPath(jobConf, new Path("/data/out"));

        JobClient.runJob(jobConf);
    }
}
