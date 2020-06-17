package com.youfan.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by li on 2019/1/19.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AnalyResult {
    private String info;//分组条件
    private Long count;//总数
}
