package bigdata.hermesfuxi.flink.example.topn;

public class OrderBean {
    // 订单id
    private String orderId;

    // 订单时间
    private Long orderTime;

    // 商品id
    private String gdsId;

    // 区域id
    private String areaId;

    // 订单金额
    private Double amount;

    public OrderBean() {
    }

    public OrderBean(String orderId, Long orderTime, String gdsId, Double amount, String areaId) {
        this.orderId = orderId;
        this.orderTime = orderTime;
        this.gdsId = gdsId;
        this.amount = amount;
        this.areaId = areaId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(Long orderTime) {
        this.orderTime = orderTime;
    }

    public String getGdsId() {
        return gdsId;
    }

    public void setGdsId(String gdsId) {
        this.gdsId = gdsId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getAreaId() {
        return areaId;
    }

    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }
}
