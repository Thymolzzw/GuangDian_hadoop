package com.it.zzw.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<Object, Text,Text,Table> {
    private String fileName;
    Table table;
    Text text;

    @Override
    protected void setup(Context context) {
        FileSplit split=(FileSplit) context.getInputSplit();
        fileName=split.getPath().getName();
        System.out.println(fileName);
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String inputLine=value.toString();
        if(fileName.startsWith("channel")){
            String[] values=inputLine.split("\\s+");
            text=new Text(values[1]);
            table=new Table();
            table.setChannelId(values[0]);
            table.setProgramId(values[1]);
            table.setAmount(Integer.parseInt(values[2]));
            table.setFlag("channel");
            table.setProgramName("");
            context.write(text,table);
        }else{
            System.out.println("inputLine"+inputLine);
            String[] values=inputLine.split("\\s+");
            text=new Text(values[0]);
            table=new Table();
            table.setProgramId(values[0]);
            table.setProgramName(values[1]);
            table.setFlag("program");
            table.setChannelId("");
            table.setAmount(0);
            context.write(text,table);
        }
    }
}
