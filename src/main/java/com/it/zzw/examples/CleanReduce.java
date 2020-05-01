package com.it.zzw.examples;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CleanReduce extends Reducer<Text,Text,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Text outPutKey=new Text();
        for(Text text:values){
            outPutKey=text;
            break;
        }
        context.write(outPutKey,NullWritable.get());
    }
}
