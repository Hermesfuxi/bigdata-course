package bigdata.hermesfuxi.hdfs.application.movierate;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieRateBean implements Writable, WritableComparable<MovieRateBean> {
    String movie;
    Double rate;
    String timeStamp;
    String uid;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movie);
        out.writeDouble(rate);
        out.writeUTF(timeStamp);
        out.writeUTF(uid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movie = in.readUTF();
        rate = in.readDouble();
        timeStamp = in.readUTF();
        uid = in.readUTF();
    }


    @Override
    public int compareTo(MovieRateBean o) {
        return o.getRate().compareTo(this.getRate());
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
