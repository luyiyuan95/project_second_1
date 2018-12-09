package com.luyi.parser.modle.service;

import com.luyi.parser.modle.dim.base.*;
import com.luyi.util.JdbcUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class DimensionOperateImpl implements DimensionOperateI {
    private static final Logger logger = Logger.getLogger(DimensionOperateImpl.class);
    private Map<String,Integer> cache = new LinkedHashMap<String, Integer>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };
    @Override
    public int getDimensionIdByDimension(BaseDimension dimension) {
        String cacheKey = buildCacheKey(dimension);
        if (this.cache.containsKey(cacheKey)){
            return this.cache.get(cacheKey);
        }
        Connection conn = JdbcUtil.getConn();
        String[] sqls = null;
        if (dimension instanceof BrowserDiemension){
            sqls = buildBrowserSqls();
        }else if (dimension instanceof PlatformDimension){
            sqls = bulidPlatformSqls();
        }else if (dimension instanceof KpiDimension){
            sqls = bulidKpiSqls();
        }else if (dimension instanceof DateDimension){
            sqls = bulidDateSqls();
        }else {
            throw new RuntimeException("该dimension不存在");
        }
        int id = -1;
        synchronized (DimensionOperateImpl.class){
            id = executedSql(sqls,conn,dimension);
        }
        this.cache.put(cacheKey,id);
        return id;
    }

    private String[] bulidDateSqls() {
        String selectsql = "select id from dimension_date where " +
                "year = ? and season = ? and month = ? and week = ? and" +
                " day = ? and calendar = ? and type = ? ";
        String insertsql = "insert into dimension_date(year,season,month,week,day,calendar,type) " +
                "value (?,?,?,?,?,?,?) ";
        return new String[]{selectsql,insertsql};
    }

    private String[] bulidPlatformSqls() {
        String selectsql = "select id from dimension_platform where platform_name = ?";
        String insertsql = "insert into dimension_platform(platform_name) value (?)";
        return new String[]{selectsql,insertsql};
    }

    private String[] bulidKpiSqls() {
        String selectsql = "select id from dimension_kpi where kpi_name = ?";
        String insertsql = "insert into dimension_kpi(kpi_name) value (?) ";
        return new String[]{selectsql,insertsql};
    }


    private String[] buildBrowserSqls() {
        String selectsql = "select id from dimension_browser where browser_name = ? and browser_version = ? ";
        String insertsql = "insert into dimension_browser(browser_name,browser_version) value (?,?) ";
        return new String[]{selectsql,insertsql};
    }

    private String buildCacheKey(BaseDimension dimension){

        StringBuffer sb = new StringBuffer();
        if (dimension instanceof BrowserDiemension){
            sb.append("browser_");
            BrowserDiemension browser = (BrowserDiemension) dimension;
            sb.append(browser.getBrowserName()).append(browser.getBrowserVersion());

        }else if (dimension instanceof PlatformDimension){
            sb.append("pl_");
            PlatformDimension pl = (PlatformDimension) dimension;
            sb.append(pl.getPlatformName());
        }else if (dimension instanceof DateDimension){
            sb.append("date_");
            DateDimension date = (DateDimension) dimension;
            sb.append(date.getYear()).append(date.getSeason()).append(date.getMonth()).append(date.getWeek())
                    .append(date.getDay())
                    .append(date.getType());
        }else if (dimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpiDimension = (KpiDimension)dimension;
            sb.append(kpiDimension.getKpiName());
        }else {
            throw new RuntimeException("该dimension不存在");
        }
        return sb.length() == 0 ? "" :sb.toString() ;
    }
    private int executedSql(String[] sqls,Connection conn,BaseDimension dimension){
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sqls[0]);
            handleArgs(ps,dimension);
            rs = ps.executeQuery();
            if (rs.next()){
                return rs.getInt("id");
            }
            //没有则插入
            ps = conn.prepareStatement(sqls[1],Statement.RETURN_GENERATED_KEYS);
            handleArgs(ps,dimension);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if (rs.next()){
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn("执行获取维度id异常",e);
        }
        return  -1;
    }

    private void handleArgs(PreparedStatement ps, BaseDimension dimension) {
        int i = 0;
        try {
            if (dimension instanceof BrowserDiemension){
                    BrowserDiemension br = (BrowserDiemension) dimension;
                    ps.setString(++i,br.getBrowserName());
                    ps.setString(++i,br.getBrowserVersion());
            }else if (dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i,kpi.getKpiName());
            }else if (dimension instanceof PlatformDimension){
                PlatformDimension pl = (PlatformDimension) dimension;
                ps.setString(++i,pl.getPlatformName());
            }else if (dimension instanceof DateDimension){
                DateDimension date = (DateDimension)dimension;
                ps.setInt(++i,date.getYear());
                ps.setInt(++i,date.getSeason());
                ps.setInt(++i,date.getMonth());
                ps.setInt(++i,date.getWeek());
                ps.setInt(++i,date.getDay());
                ps.setDate(++i,new Date(date.getCalendar().getTime()));
                ps.setString(++i,date.getType());
            }else {
                throw new RuntimeException("该dimension不存在");
            }
        } catch (SQLException e) {
            logger.warn("sql参数中断异常",e);
        }
    }
}
