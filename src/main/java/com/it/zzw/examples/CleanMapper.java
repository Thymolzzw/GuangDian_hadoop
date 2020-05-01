package com.it.zzw.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CleanMapper extends Mapper<Object, Text,Text,Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] values=value.toString().split(" ");
        context.write(new Text(values[0]),value);
    }
}
