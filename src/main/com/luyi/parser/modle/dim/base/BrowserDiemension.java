package com.luyi.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BrowserDiemension extends BaseDimension {
    private int id;
    private String browserName;
    private String browserVersion;

    public BrowserDiemension() {}

    public BrowserDiemension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDiemension(int id, String browserName, String browserVersion) {
        this(browserName,browserVersion);
        this.id = id;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.browserName);
        dataOutput.writeUTF(this.browserVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readInt();
        this.browserName=dataInput.readUTF();
        this.browserVersion=dataInput.readUTF();
    }
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        BrowserDiemension temp = (BrowserDiemension)o;
        int tmp = this.id - temp.id;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.browserName.compareTo(temp.browserName);
        if (tmp != 0){
            return tmp;
        }
        return this.browserVersion.compareTo(temp.browserVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDiemension that = (BrowserDiemension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browserVersion, that.browserVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, browserName, browserVersion);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    @Override
    public String toString() {
        return "BrowserDiemension{" +
                "id=" + id +
                ", browserName='" + browserName + '\'' +
                ", browserVersion='" + browserVersion + '\'' +
                '}';
    }
}
