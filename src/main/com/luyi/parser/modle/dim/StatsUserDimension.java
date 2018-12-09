package com.luyi.parser.modle.dim;

import com.luyi.parser.modle.dim.base.BaseDimension;
import com.luyi.parser.modle.dim.base.BrowserDiemension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsUserDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDiemension browserDiemension = new BrowserDiemension();

    public StatsUserDimension() { }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDiemension browserDiemension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDiemension = browserDiemension;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statsCommonDimension.write(dataOutput);
        this.browserDiemension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommonDimension.readFields(dataInput);
        this.browserDiemension.readFields(dataInput);
    }
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0){
            return tmp;
        }
        return this.browserDiemension.compareTo(other.browserDiemension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(browserDiemension, that.browserDiemension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(statsCommonDimension, browserDiemension);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDiemension getBrowserDiemension() {
        return browserDiemension;
    }

    public void setBrowserDiemension(BrowserDiemension browserDiemension) {
        this.browserDiemension = browserDiemension;
    }

    @Override
    public String toString() {
        return "StatsUserDimension{" +
                "statsCommonDimension=" + statsCommonDimension +
                ", browserDiemension=" + browserDiemension +
                '}';
    }
}
