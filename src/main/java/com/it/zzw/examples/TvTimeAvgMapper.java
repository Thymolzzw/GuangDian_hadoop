package com.it.zzw.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TvTimeAvgMapper extends Mapper<Object,Text,Text,TVTime> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] values=value.toString().split(" ");
        Text text=new Text(values[1]);
        TVTime tvTime=new TVTime();
        tvTime.setDay(Integer.parseInt(values[2]));
        tvTime.setNight(Integer.parseInt(values[3]));
        context.write(text,tvTime);
    }
}
