package com.luyi.parser.modle.dim.base.value.reduce;

import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.base.value.StatsBaseOutputWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceOutputWritable extends StatsBaseOutputWritable {

    private MapWritable value = new MapWritable();
    private KpiTypeEnum kpi;

    public ReduceOutputWritable(MapWritable value, KpiTypeEnum kpi) {
        this.value = value;
        this.kpi = kpi;
    }

    public ReduceOutputWritable() { }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.value.write(dataOutput);
        WritableUtils.writeEnum(dataOutput,this.kpi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value.readFields(dataInput);
        WritableUtils.readEnum(dataInput,KpiTypeEnum.class);
    }

    @Override
    public KpiTypeEnum getKpi() {
        return this.kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiTypeEnum kpi) {
        this.kpi = kpi;
    }
}
