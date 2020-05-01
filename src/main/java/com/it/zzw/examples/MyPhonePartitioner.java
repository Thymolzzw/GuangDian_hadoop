package com.it.zzw.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPhonePartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text o, Text o2, int i) {
        int partition = 3;

        String phoneNum = o.toString();
        if (phoneNum.startsWith("136")) {
            partition = 0;
        } else if (phoneNum.startsWith("137")) {
            partition = 1;
        } else if (phoneNum.startsWith("138")) {
            partition = 2;
        } else{
            partition = 3;
        }
        return partition;
    }


}
