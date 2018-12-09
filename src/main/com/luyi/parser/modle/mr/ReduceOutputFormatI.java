package com.luyi.parser.modle.mr;

import com.luyi.parser.modle.dim.base.BaseDimension;
import com.luyi.parser.modle.dim.base.value.StatsBaseOutputWritable;
import com.luyi.parser.modle.service.DimensionOperateI;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

public interface ReduceOutputFormatI {
    void outputWritter(Configuration conf, BaseDimension key, StatsBaseOutputWritable value, PreparedStatement ps
    , DimensionOperateI operateI);
}
