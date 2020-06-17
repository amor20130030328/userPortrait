package com.gy.logic;

import java.util.ArrayList;

/**
 * 该列主要用于保存特征信息以及标签值
 * labels 主要保存标签值
 */
public class CreateDataSet extends  Matrix{

    public ArrayList<String> labels;

    public CreateDataSet(){
        super();
        labels = new ArrayList<>();
    }
}
