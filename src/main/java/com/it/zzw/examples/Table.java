package com.it.zzw.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Table implements Writable {
    private String channelId;
    private String programId;
    private int amount;
    private String programName;
    private String flag;
    public Table() {
        super();
    }
    public Table(String channelId, String programId,
                 int amount, String programName, String flag) {
        super();
        this.channelId = channelId;
        this.programId = programId;
        this.amount = amount;
        this.programName = programName;
        this.flag = flag;
    }
    public String getChannelId() {
        return channelId;
    }
    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
    public String getProgramId() {
        return programId;
    }
    public void setProgramId(String programId) {
        this.programId = programId;
    }
    public int getAmount() {
        return amount;
    }
    public void setAmount(int amount) {
        this.amount = amount;
    }
    public String getProgramName() {
        return programName;
    }
    public void setProgramName(String programName) {
        this.programName = programName;
    }
    public String getFlag() {
        return flag;
    }
    public void setFlag(String flag) {
        this.flag = flag;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(channelId);
        dataOutput.writeUTF(programId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(programName);
        dataOutput.writeUTF(flag);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        channelId=dataInput.readUTF();
        programId=dataInput.readUTF();
        amount=dataInput.readInt();
        programName=dataInput.readUTF();
        flag=dataInput.readUTF();
    }
    @Override
    public String toString() {
        return channelId+"\t"+programName+"\t"+amount;
    }
}
