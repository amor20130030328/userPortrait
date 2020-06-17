package com.gy.userportrait.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultMessage {

    private String status;   //状态  fail , success
    private String message;   //消息内容
}
