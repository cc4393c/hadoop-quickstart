package com.mashibing.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);

        // 工具类帮我们把-D 等等的属性直接set到conf，会留下commandOptions
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
//        String[] othargs = parser.getRemainingArgs();

        // 让框架知道是在windows平台运行
        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.set("mapreduce.framework.name", "local");
        System.out.println(conf.get("mapreduce.framework.name"));
        Job job = Job.getInstance(conf);

        // Create a new Job
//        Job job = Job.getInstance();
//        job.setJar("C:\\Users\\ccr13\\IdeaProjects\\BigData\\msbhadoop\\target\\hadoop-hdfs-1.0-SNAPSHOT.jar");
        job.setJarByClass(MyWordCount.class);

        // Specify various job-specific parameters
        job.setJobName("mashibing");

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));

        Path infile = new Path("/data/wc/input");//new Path("/data/wc/input");
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path("/data/wc/output");//new Path("/data/wc/output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

//        job.setNumReduceTasks(2);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
