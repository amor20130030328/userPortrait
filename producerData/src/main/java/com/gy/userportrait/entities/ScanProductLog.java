package com.gy.userportrait.entities;

import lombok.Data;

@Data
public class ScanProductLog {

    private int productid;//商品id
    private int producttypeid;//商品类别id
    private String scantime;//浏览时间
    private String staytime;//停留时间
    private int userid;//用户id
    private int usetype;//终端类型：0、pc端；1、移动端；2、小程序端'
    private String ip;// 用户ip
    private String brand;//品牌

    @Override
    public String toString() {
        return "ScanProductLog{" +
                "productid=" + productid +
                ", producttypeid=" + producttypeid +
                ", scantime='" + scantime + '\'' +
                ", staytime='" + staytime + '\'' +
                ", userid=" + userid +
                ", usetype=" + usetype +
                ", ip='" + ip + '\'' +
                ", brand='" + brand + '\'' +
                "}##"+1+"##" + System.currentTimeMillis();
    }
}
