package com.luyi.parser.modle.mr.nu;

import com.luyi.common.DateTypeEnum;
import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.StatsCommonDimension;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.BrowserDiemension;
import com.luyi.parser.modle.dim.base.DateDimension;
import com.luyi.parser.modle.dim.base.KpiDimension;
import com.luyi.parser.modle.dim.base.PlatformDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.util.UserAgentUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyNewUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputWritable> {
    StatsUserDimension statsUserDimension = new StatsUserDimension();
    PlatformDimension platformDimension = new PlatformDimension();
    KpiDimension kpiDimension = new KpiDimension(KpiTypeEnum.NEW_USER.kpiName);
    TimeOutputWritable timeOutputWritable = new TimeOutputWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



            String[] lines = value.toString().split("\001");

            platformDimension.setPlatformName(lines[2]);
            DateDimension dateDimension = DateDimension.bulitDateDimension(Long.parseLong(lines[10]),DateTypeEnum.DAY);
            UserAgentUtil.UserAgentInfo browser = UserAgentUtil.parserUserAgent(lines[5]);

            StatsCommonDimension statsCommonDimension  = this.statsUserDimension.getStatsCommonDimension();
            BrowserDiemension browserDiemension = this.statsUserDimension.getBrowserDiemension();

            browserDiemension.setBrowserName(browser.getBrowserName());
            browserDiemension.setBrowserVersion(browser.getBrowserVersion());


            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            statsCommonDimension.setKpiDimension(kpiDimension);


            this.statsUserDimension.setBrowserDiemension(browserDiemension);
            this.statsUserDimension.setStatsCommonDimension(statsCommonDimension);
            timeOutputWritable.setId(lines[6]);
            timeOutputWritable.setTime(Long.parseLong(lines[10]));

            System.out.println(statsUserDimension);
            System.out.println(timeOutputWritable);
            context.write(statsUserDimension,timeOutputWritable);
    }
}
