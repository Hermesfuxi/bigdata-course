package bigdata.hermesfuxi.flink.windows.custom;

public class TopInput {
    private String eventId;
    private String dateDay;
    private String timeStamp;
    private String uid;
    private String weekTag;
    private String monthTag;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getDateDay() {
        return dateDay;
    }

    public void setDateDay(String dateDay) {
        this.dateDay = dateDay;
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

    public String getWeekTag() {
        return weekTag;
    }

    public void setWeekTag(String weekTag) {
        this.weekTag = weekTag;
    }

    public String getMonthTag() {
        return monthTag;
    }

    public void setMonthTag(String monthTag) {
        this.monthTag = monthTag;
    }
}
