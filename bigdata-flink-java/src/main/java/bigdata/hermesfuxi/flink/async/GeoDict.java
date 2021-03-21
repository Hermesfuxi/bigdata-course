package bigdata.hermesfuxi.flink.async;

public class GeoDict {
    private String geoHashCode;
    private String province;
    private String city;
    private String region;

    public GeoDict(String geohash, String province, String city, String region) {
        this.geoHashCode = geohash;
        this.province = province;
        this.city = city;
        this.region = region;
    }

    public String getGeoHashCode() {
        return geoHashCode;
    }

    public void setGeoHashCode(String geoHashCode) {
        this.geoHashCode = geoHashCode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "GeoDict{" +
                "geohash='" + geoHashCode + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
