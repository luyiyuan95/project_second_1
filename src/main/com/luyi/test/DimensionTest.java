import com.luyi.common.DateTypeEnum;
import com.luyi.parser.modle.dim.StatsCommonDimension;
import com.luyi.parser.modle.dim.base.BrowserDiemension;
import com.luyi.parser.modle.dim.base.DateDimension;
import com.luyi.parser.modle.service.DimensionOperateImpl;

public class DimensionTest {
    public static void main(String[] args) {
/*        BrowserDiemension br = new BrowserDiemension("IE","9.0");
        DimensionOperateImpl dimensionOperate = new DimensionOperateImpl();
        System.out.println(dimensionOperate.getDimensionIdByDimension(br));*/
        StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
        DateDimension dateDimension = new DateDimension();
        DateDimension d = dateDimension.bulitDateDimension(Long.parseLong("1543849224077"),DateTypeEnum.MONTH);
        statsCommonDimension.setDateDimension(d);
        System.out.println(statsCommonDimension.getDateDimension());
    }
}
