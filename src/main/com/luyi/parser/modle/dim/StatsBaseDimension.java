package com.luyi.parser.modle.dim;

import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class StatsBaseDimension extends BaseDimension {
    public KpiTypeEnum getKpi(){return null;};
}
