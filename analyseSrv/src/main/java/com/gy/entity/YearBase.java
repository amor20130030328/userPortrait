package com.gy.entity;

import lombok.Data;

@Data
public class YearBase {

    private String yearType;  //年代类型
    private Long count;     //数量
    private String groupfield;  //分组字段
}
