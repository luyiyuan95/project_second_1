package com.luyi.parser.modle.dim.base.value.map;

import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.base.value.StatsBaseOutputWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeOutputWritable extends StatsBaseOutputWritable {

   private String id;
   private long time;

    public TimeOutputWritable(String id, long time) {
        this.id = id;
        this.time = time;
    }

    public TimeOutputWritable() { }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time = dataInput.readLong();
    }

    @Override
    public KpiTypeEnum getKpi() {
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "TimeOutputWritable{" +
                "id='" + id + '\'' +
                ", time=" + time +
                '}';
    }
}
