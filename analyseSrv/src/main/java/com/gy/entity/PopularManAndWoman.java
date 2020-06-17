package com.gy.entity;

import lombok.Data;

import java.util.List;

@Data
public class PopularManAndWoman {

    private String popularType;     //1.潮男 ; 2.潮女
    private String userId;        //用户id
    private long count;
    private String groupbyfiled;
    private List<PopularManAndWoman> list;

}
