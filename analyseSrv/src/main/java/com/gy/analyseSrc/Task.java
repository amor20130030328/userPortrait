package com.gy.analyseSrc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class Task {

    public static void main(String[] args) {

        ParameterTool parames = ParameterTool.fromArgs(args);
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getConfig().setGlobalJobParameters(parames);

        DataSet<String> text = executionEnvironment.readTextFile(parames.get("input"));


    }
}
