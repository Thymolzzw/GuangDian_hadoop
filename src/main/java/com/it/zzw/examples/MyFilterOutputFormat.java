package com.it.zzw.examples;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MyFilterOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text,NullWritable> getRecordWriter(TaskAttemptContext job) {
        return new MyFilterRecordWriter(job);
    }
}
