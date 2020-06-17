package com.gy.analyseSrc;

import com.gy.entity.SexPreInfo;
import com.gy.map.SexPreMap;
import com.gy.map.SexPreSaveMap;
import com.gy.reduce.SexPreReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

public class SexPreTask {

    public static void main(String[] args) {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setGlobalJobParameters(parameterTool);

        DataSet<String> input = environment.readTextFile(parameterTool.get("input"));
        DataSet<SexPreInfo> map = input.map(new SexPreMap());
        DataSet<ArrayList<Double>> reduce = map.groupBy("groupfield").reduceGroup(new SexPreReduce());


        try {
            List<ArrayList<Double>> resultlist = reduce.collect();
            int groupSize = resultlist.size();

            Map<Integer, Double> summap = new TreeMap<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });

            for(ArrayList<Double> array : resultlist){
                for(int i = 0 ; i< array.size();i++){
                    double pre = summap.get(i) == null ? 0D : summap.get(i);
                    summap.put(i,pre + array.get(i));
                }
            }

            ArrayList<Double> finalweight = new ArrayList<>();
            Set<Map.Entry<Integer, Double>> set = summap.entrySet();
            for(Map.Entry<Integer, Double> entry : set){
                Integer key = entry.getKey();
                Double sumvalue = entry.getValue();
                double finalValue = sumvalue / groupSize;
                finalweight.add(finalValue);
            }

            DataSet<String> input2 = environment.readTextFile(parameterTool.get("input2"));
            input2.map(new SexPreSaveMap(finalweight));
            environment.execute("SexPreTask");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
