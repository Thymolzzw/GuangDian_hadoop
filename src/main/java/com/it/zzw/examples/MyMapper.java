package com.it.zzw.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object,Text,Text,IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words=value.toString().split(" ");
        for(String word:words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
