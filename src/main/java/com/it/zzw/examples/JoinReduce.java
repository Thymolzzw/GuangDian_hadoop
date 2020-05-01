package com.it.zzw.examples;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class JoinReduce extends Reducer<Text,Table,Table, NullWritable> {
    @Override
    protected void reduce(Text programId, Iterable<Table> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Table> channelTableList= new ArrayList<>();
        Table programTable=null;
        for(Table table:values){
            if(table.getFlag().equals("channel")){
                Table tmp=new Table();
                tmp.setAmount(table.getAmount());
                tmp.setChannelId(table.getChannelId());
                tmp.setProgramName(table.getProgramName());
                tmp.setFlag(table.getFlag());
                tmp.setProgramId(table.getProgramId());
                channelTableList.add(tmp);
            }else{
                programTable=new Table();
                programTable.setProgramId(table.getProgramId());
                programTable.setFlag(table.getFlag());
                programTable.setProgramName(table.getProgramName());
                programTable.setChannelId(table.getChannelId());
                programTable.setAmount(table.getAmount());
            }
        }
        for(Table table:channelTableList){
            if(programTable!=null){
                table.setProgramName(programTable.getProgramName());
            }
            context.write(table,NullWritable.get());
        }
    }
}
