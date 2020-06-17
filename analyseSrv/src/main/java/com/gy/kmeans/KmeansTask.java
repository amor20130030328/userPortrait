package com.gy.kmeans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

public class KmeansTask {

    public static void main(String[] args) {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setGlobalJobParameters(parameterTool);

        DataSet<String> input = environment.readTextFile(parameterTool.get("input"));
        DataSet<Kmeans> map = input.map(new KmeansMap());
        DataSet<ArrayList<Point>> reduce = map.groupBy("groupfield").reduceGroup(new KmeanReduce());


        try {
            List<ArrayList<Point>> resultlist = reduce.collect();
            ArrayList<float[]> dataset = new ArrayList<>();
            for(ArrayList<Point> array : resultlist){
                for(Point point: array){
                    dataset.add(point.getLocalArray());
                }
            }

            KmeansRun kRun = new KmeansRun(6, dataset);
            Set<Cluster> clusterSet = kRun.run();
            List<Point> finalClusterCenter = new ArrayList<>();
            int count = 100;
            for(Cluster cluster : clusterSet){
                Point point = cluster.getCenter();
                point.setId(count++);
                finalClusterCenter.add(point);
            }

            DataSet<Point> finalMap = input.map(new KmeansFinalMap(finalClusterCenter));
            finalMap.writeAsText(parameterTool.get("out"));
            environment.execute("kmeans Analy");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
