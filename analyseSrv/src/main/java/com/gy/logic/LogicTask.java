package com.gy.logic;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

public class LogicTask  {

    public static void main(String[] args) {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(parameterTool);
        DataSet<String> text = environment.readTextFile(parameterTool.get("input"));

        DataSet<LogicInfo> mapDataSet = text.map(new LogicMap());
        DataSet<ArrayList<Double>> reduceDataSet = mapDataSet.groupBy("groupbyfield").reduceGroup(new LogicReduce());

        try {
            List<ArrayList<Double>> resultlist = reduceDataSet.collect();

            int groupSize = resultlist.size();
            Map<Integer, Double> summap = new TreeMap<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });

            for (ArrayList<Double> array : resultlist) {
                for (int i = 0 ; i < array.size();i++){
                    double pre = summap.get(i) == null ? 0d : summap.get(i);
                    summap.put(i,pre + array.get(i));
                }
            }

            ArrayList<Double> finalWeight = new ArrayList<>();
            Set<Map.Entry<Integer, Double>> set = summap.entrySet();
            for(Map.Entry<Integer, Double> mapentry : set){
                Integer key = mapentry.getKey();
                Double sumvalue = mapentry.getValue();
                Double finalvalue = sumvalue/groupSize;
                finalWeight.add(finalvalue);
            }
            environment.execute("LogicTask analy");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
