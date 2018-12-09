package com.luyi.parser.modle.mr.nu;

import com.luyi.common.DateTypeEnum;
import com.luyi.common.EventEnum;
import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.StatsCommonDimension;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.BrowserDiemension;
import com.luyi.parser.modle.dim.base.DateDimension;
import com.luyi.parser.modle.dim.base.KpiDimension;
import com.luyi.parser.modle.dim.base.PlatformDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.io.IOException;

public class NewUserMapper  extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputWritable> {
    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private static final StatsUserDimension k = new StatsUserDimension();
    private static final TimeOutputWritable v = new TimeOutputWritable();
    private KpiDimension newUserKpi = new KpiDimension(KpiTypeEnum.NEW_USER.kpiName);
    private KpiDimension newbrowserUserKpi = new KpiDimension(KpiTypeEnum.BROWSER_NEW_USER.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\001");
        String envent = fields[0];
        String uid = fields[6];
        String sTime = fields[15];
        String platfrome = fields[2];
        String browserName = fields[28];
        String browserVersion = fields[29];

        if (!envent.equals(EventEnum.LANUCH.alias)){
            //logger.warn("事件不是lanuch事件: " + line);
            return;
        }
        if (StringUtils.isEmpty(sTime) || StringUtils.isEmpty(uid)){
            logger.warn("stime is null:"+sTime+" and uid is null:"+uid);
        }
        long longOfTime = Long.valueOf(sTime);

        this.v.setId(uid);
        DateDimension dateDimension = DateDimension.bulitDateDimension(longOfTime,DateTypeEnum.DAY);
        PlatformDimension platformDimension = new PlatformDimension(platfrome);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        BrowserDiemension dafaultBrowser = new BrowserDiemension("","");

        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(newUserKpi);
        statsCommonDimension.setPlatformDimension(platformDimension);

        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setBrowserDiemension(dafaultBrowser);

        context.write(this.k,this.v);

        BrowserDiemension browser = new BrowserDiemension(browserName,browserVersion);
        this.k.setBrowserDiemension(browser);
        statsCommonDimension.setKpiDimension(newbrowserUserKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        context.write(this.k,this.v);

    }
}
