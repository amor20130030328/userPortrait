package com.gy.entity;

import lombok.Data;

@Data
public class CarrierInfo {

    private String carrier;  //运营商
    private Long count;    //数量
    private String  groupField;    //分组

}
