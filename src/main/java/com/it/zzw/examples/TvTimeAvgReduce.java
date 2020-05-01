package com.it.zzw.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TvTimeAvgReduce extends Reducer<Text,TVTime,Text,TVTime> {
    @Override
    protected void reduce(Text man, Iterable<TVTime> tvTimes, Context context)
            throws IOException, InterruptedException {
        Integer daySum=0;
        Integer nightSum=0;
        int num=0;
        for(TVTime tvTime:tvTimes){
            daySum+=tvTime.getDay();
            nightSum+=tvTime.getNight();
            num++;
        }
        TVTime tvTimeAns=new TVTime();
        tvTimeAns.setDay(daySum/num);
        tvTimeAns.setNight(nightSum/num);
        context.write(man,tvTimeAns);
    }
}
