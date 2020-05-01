package com.it.zzw.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TVTime implements Writable {
    private Integer day;
    private Integer night;
    public TVTime(){
        super();
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(day);
        dataOutput.writeInt(night);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.day=dataInput.readInt();
        this.night=dataInput.readInt();
    }

    @Override
    public String toString() {
        return day+"\t"+night;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getNight() {
        return night;
    }

    public void setNight(Integer night) {
        this.night = night;
    }
}
