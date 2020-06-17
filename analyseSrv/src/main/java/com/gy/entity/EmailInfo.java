package com.gy.entity;

import lombok.Data;

@Data
public class EmailInfo {

    private String emailType ;   //邮箱类型
    private Long count ;      //数量
    private String groupField;   //分组字段

}
