package com.it.zzw.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyAudienceRate implements Writable,Comparable<MyAudienceRate> {
    private int rate;
    private String content;
    public MyAudienceRate() {super();}
    public MyAudienceRate(int rate, String content) {
        super();
        this.rate = rate;
        this.content = content;
    }
    public int getRate() {
        return rate;
    }
    public void setRate(int rate) {
        this.rate = rate;
    }
    public String getContent() {
        return content;
    }
    public void setContent(String content) {
        this.content = content;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(rate);
        dataOutput.writeUTF(content);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rate=dataInput.readInt();
        content=dataInput.readUTF();
    }
    @Override
    public String toString() {
        return content;
    }
    @Override
    public int compareTo(MyAudienceRate o) {
        return o.getRate()-rate;
    }
}
