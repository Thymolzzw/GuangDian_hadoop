package com.it.zzw.examples;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class TopFiveReduce extends Reducer<NullWritable, Text,Text,NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<MyAudienceRate> arrayList=new ArrayList<>();
        for(Text text:values){
            MyAudienceRate myAudienceRate=new MyAudienceRate();
            String[] valueList=text.toString().split(" ");
            myAudienceRate.setRate(Integer.parseInt(valueList[2]));
            myAudienceRate.setContent(text.toString());
            arrayList.add(myAudienceRate);
        }
        Collections.sort(arrayList);
        for(int i=0;i<5;i++){
            context.write(new Text(arrayList.get(i).getContent()),NullWritable.get());
        }
    }
}
