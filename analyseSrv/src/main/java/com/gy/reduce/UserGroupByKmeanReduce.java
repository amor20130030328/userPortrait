package com.gy.reduce;

import com.gy.entity.UserGroupInfo;
import com.gy.kmeans.Cluster;
import com.gy.kmeans.KmeansRun;
import com.gy.kmeans.Point;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class UserGroupByKmeanReduce implements GroupReduceFunction<UserGroupInfo,ArrayList<Point>> {
    @Override
    public void reduce(Iterable<UserGroupInfo> values, Collector<ArrayList<Point>> out) throws Exception {

        Iterator<UserGroupInfo> iterator = values.iterator();
        ArrayList<float []> dataset = new ArrayList<>();
        while(iterator.hasNext()){
            UserGroupInfo userGroupInfo = iterator.next();
            float[] f = new float[]{Float.valueOf(userGroupInfo.getUserid()+""),Float.valueOf(userGroupInfo.getAvramount()+""),Float.valueOf(userGroupInfo.getMaxamount()+""),Float.valueOf(userGroupInfo.getDays()),
                    Float.valueOf(userGroupInfo.getBuytype1()),Float.valueOf(userGroupInfo.getBuytype2()),Float.valueOf(userGroupInfo.getBuytype3()),
                    Float.valueOf(userGroupInfo.getBuytime1()),Float.valueOf(userGroupInfo.getBuytime2()),Float.valueOf(userGroupInfo.getBuytime3()),
                    Float.valueOf(userGroupInfo.getBuytime4())};

            dataset.add(f);
        }

        KmeansRun kmeansRun = new KmeansRun(6,dataset);
        Set<Cluster> clusterSet = kmeansRun.run();
        ArrayList arrayList = new ArrayList();
        for(Cluster cluster : clusterSet){
            arrayList.add(cluster.getCenter());
        }
        out.collect(arrayList);
    }
}
