package com.it.zzw.examples;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream henan=null;
    FSDataOutputStream sichuan=null;

    public MyFilterRecordWriter(TaskAttemptContext job) {
        FileSystem fs;
        try{
            fs=FileSystem.get(job.getConfiguration());
            Path pathOfSichuan=new Path("D:\\IDEA_workspace\\myFilter\\sichuan");
            Path pathOfHenan=new Path("D:\\IDEA_workspace\\myFilter\\henan");
            henan=fs.create(pathOfHenan);
            sichuan=fs.create(pathOfSichuan);

        }catch (IOException e){
            e.printStackTrace();
        }
    }
    @Override
    public synchronized void write(Text text, NullWritable nullWritable)
            throws IOException, InterruptedException {
        if(text.toString().contains("四川")){
            sichuan.write(text.toString().getBytes());
            sichuan.write("\r\n".getBytes());
        }else{
            henan.write(text.toString().getBytes());
            henan.write("\r\n".getBytes());
        }
    }
    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
        IOUtils.closeStream(sichuan);
        IOUtils.closeStream(henan);
    }
}
