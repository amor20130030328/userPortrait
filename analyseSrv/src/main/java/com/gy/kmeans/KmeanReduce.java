package com.gy.kmeans;

import com.gy.kmeans.Cluster;
import com.gy.kmeans.Kmeans;
import com.gy.kmeans.KmeansRun;
import com.gy.kmeans.Point;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class KmeanReduce implements GroupReduceFunction<Kmeans,ArrayList<Point>> {
    @Override
    public void reduce(Iterable<Kmeans> values, Collector<ArrayList<Point>> out) throws Exception {
        Iterator<Kmeans> iterator = values.iterator();
        ArrayList<float[]> dataset = new ArrayList<>();
        while(iterator.hasNext()){
            Kmeans kmeans = iterator.next();
            float [] f =  new float[]{Float.valueOf(kmeans.getVariable1()),Float.valueOf(kmeans.getVariable2()),Float.valueOf(kmeans.getVariable3())};
            dataset.add(f);
        }

        KmeansRun kmeansRun = new KmeansRun(6, dataset);
        Set<Cluster> clusterSet = kmeansRun.run();
        ArrayList<Point> arrayList = new ArrayList<>();
        for(Cluster cluster : clusterSet){
            arrayList.add(cluster.getCenter());
        }
        out.collect(arrayList);
    }
}
