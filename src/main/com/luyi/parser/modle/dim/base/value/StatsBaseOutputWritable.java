package com.luyi.parser.modle.dim.base.value;

import com.luyi.common.KpiTypeEnum;
import org.apache.hadoop.io.Writable;

public abstract class StatsBaseOutputWritable implements Writable {

    public abstract KpiTypeEnum getKpi();
}
