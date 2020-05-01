package com.it.zzw.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class GuangDian {

    //任务6
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        args=new String[]{
                "D:\\IDEA_workspace\\data5.txt","D:\\IDEA_workspace\\topFive"
        };
        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration,"top five");
        job.setJarByClass(GuangDian.class);
        job.setMapperClass(TopFiveMapper.class);
        job.setReducerClass(TopFiveReduce.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(MyAudienceRate.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }


    //任务5
//    public static void main(String[] args) throws IOException,
//            ClassNotFoundException, InterruptedException {
//        args=new String[]{
//                "D:\\IDEA_workspace\\data4.txt","D:\\IDEA_workspace\\cleaned"
//        };
//        Configuration configuration=new Configuration();
//        Job job=Job.getInstance(configuration,"clean data");
//        job.setJarByClass(GuangDian.class);
//        job.setMapperClass(CleanMapper.class);
//        job.setReducerClass(CleanReduce.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//
//
//        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        System.exit(job.waitForCompletion(true)?0:1);
//    }


    //任务4
//    public static void main(String[] args) throws IOException,
//            ClassNotFoundException, InterruptedException {
//        args=new String[]{
//                "D:\\IDEA_workspace\\join\\","D:\\IDEA_workspace\\joinoutput"
//        };
//        Configuration configuration=new Configuration();
//        Job job=Job.getInstance(configuration,"join");
//        job.setJarByClass(GuangDian.class);
//        job.setMapperClass(JoinMapper.class);
//        job.setReducerClass(JoinReduce.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Table.class);
//
//        job.setOutputKeyClass(Table.class);
//        job.setOutputValueClass(NullWritable.class);
//
//
//        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        System.exit(job.waitForCompletion(true)?0:1);
//    }

    //任务3
//    public static void main(String[] args) throws IOException,
//            ClassNotFoundException, InterruptedException {
//        args=new String[]{
//                "D:\\IDEA_workspace\\data3.txt","D:\\IDEA_workspace\\myFilter"
//        };
//        Configuration configuration=new Configuration();
//        Job job=Job.getInstance(configuration,"myFilter");
//        job.setJarByClass(GuangDian.class);
//        job.setMapperClass(FilterMapper.class);
//        job.setReducerClass(FilterReduce.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//        job.setOutputFormatClass(MyFilterOutputFormat.class);
//        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        System.exit(job.waitForCompletion(true)?0:1);
//    }

    //任务2
//    public static void main(String[] args) throws IOException,
//            ClassNotFoundException, InterruptedException {
//        args=new String[]{
//                "D:\\IDEA_workspace\\data1.txt","D:\\IDEA_workspace\\phonepart"
//        };
//        Configuration configuration=new Configuration();
//        Job job=Job.getInstance(configuration,"phone partition");
//        job.setJarByClass(GuangDian.class);
//        job.setMapperClass(MyPhoneMapper.class);
//        job.setReducerClass(MyPhoneReduce.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        job.setPartitionerClass(MyPhonePartitioner.class);
//        job.setNumReduceTasks(4);
//
//        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        System.exit(job.waitForCompletion(true)?0:1);
//    }

    /*任务1*/
//    public static void main(String[] args) throws IOException,
//            ClassNotFoundException, InterruptedException {
//        args=new String[]{
//            "D:\\IDEA_workspace\\data.txt","D:\\IDEA_workspace\\tvtime"
////        };
//        Configuration configuration=new Configuration();
//        Job job=Job.getInstance(configuration,"avg time");
//        job.setJarByClass(GuangDian.class);
//        job.setMapperClass(TvTimeAvgMapper.class);
//        job.setReducerClass(TvTimeAvgReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(TVTime.class);
//        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        System.exit(job.waitForCompletion(true)?0:1);
//    }
}
