package com.gy.logic;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

import static com.gy.logic.Logistic.gradAscent1;

public class LogicReduce implements GroupReduceFunction<LogicInfo,ArrayList<Double>> {
    @Override
    public void reduce(Iterable<LogicInfo> iterable, Collector<ArrayList<Double>> out) throws Exception {

        Iterator<LogicInfo> iterator = iterable.iterator();
        CreateDataSet trainingSet = new CreateDataSet();
        while(iterator.hasNext()){
            LogicInfo logicInfo = iterator.next();
            String variable1 = logicInfo.getVariable1();
            String variable2 = logicInfo.getVariable2();
            String variable3 = logicInfo.getVariable3();
            String label = logicInfo.getLabase();

            ArrayList<String> as = new ArrayList<>();
            as.add(variable1);
            as.add(variable2);
            as.add(variable3);

            trainingSet.data.add(as);
            trainingSet.labels.add(label);

        }
        ArrayList<Double> weights  = gradAscent1(trainingSet, trainingSet.labels, 500);
        out.collect(weights);

    }
}
