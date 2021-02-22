package bigdata.hermesfuxi.hdfs.application.userorder.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserOrderBean implements Writable {
    private String orderId;
    private String uid;
    private String userName;
    private Integer age;
    private String sex;
    private String dishName;

    public UserOrderBean() {
    }

    public UserOrderBean(String orderId, String uid, String userName, Integer age, String sex, String dishName) {
        this.orderId = orderId;
        this.uid = uid;
        this.userName = userName;
        this.age = age;
        this.sex = sex;
        this.dishName = dishName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(uid);
        out.writeUTF(userName);
        out.writeInt(age);
        out.writeUTF(sex);
        out.writeUTF(dishName);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        uid = in.readUTF();
        userName = in.readUTF();
        age = in.readInt();
        sex = in.readUTF();
        dishName = in.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "," + uid + "," + userName + "," + age + "," + sex + "," + dishName;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getDishName() {
        return dishName;
    }

    public void setDishName(String dishName) {
        this.dishName = dishName;
    }
}
