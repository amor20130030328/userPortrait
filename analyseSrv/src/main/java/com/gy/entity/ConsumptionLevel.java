package com.gy.entity;

import lombok.Data;

@Data
public class ConsumptionLevel {

    private String consumptiontype;   //消费水平 高水平  中等水平  低水平
    private Long  count ;     //数量
    private String groupbyfield;   //分组字段
    private String userId;       //用户Id
    private String amounttotal;   //金额




}
