package com.gy.analyseSrc;

import com.gy.entity.UserGroupInfo;
import com.gy.kmeans.Cluster;
import com.gy.kmeans.KmeansRun;
import com.gy.kmeans.Point;
import com.gy.map.KmeansFinalClusterGroupMap;
import com.gy.map.UserGroupMap;
import com.gy.map.UserGroupMapByReduce;
import com.gy.reduce.UserGroupByKmeanReduce;
import com.gy.reduce.UserGroupInfoReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class UserGroupTask {

    public static void main(String[] args) {

        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setGlobalJobParameters(params);
        DataSet<String> input = environment.readTextFile(params.get("input"));

        DataSet<UserGroupInfo> map = input.map(new UserGroupMap());
        DataSet<UserGroupInfo> reduce = map.groupBy("groupfield").reduce(new UserGroupInfoReduce());
        DataSet<UserGroupInfo> map2 = reduce.map(new UserGroupMapByReduce());
        DataSet<ArrayList<Point>> finalresult= map2.groupBy("groupfield").reduceGroup(new UserGroupByKmeanReduce());


        try {
            List<ArrayList<Point>> resultlist = finalresult.collect();
            ArrayList<float[]> dataset = new ArrayList<>();
            for(ArrayList<Point> array : resultlist){
                for(Point point : array){
                    dataset.add(point.getLocalArray());
                }
            }

            KmeansRun kmeansRun = new KmeansRun(6,dataset);
            Set<Cluster> clusterSet = kmeansRun.run();
            List<Point> finalClusterCenter = new ArrayList<>();
            int count = 100;
            for(Cluster cluster : clusterSet){
                Point point = cluster.getCenter();
                point.setId(count + 1);
                finalClusterCenter.add(point);
            }

            DataSet<Point> finalMap = map2.map(new KmeansFinalClusterGroupMap(finalClusterCenter));
            environment.execute("usergroupTask analy");

        }catch (Exception e){
            e.printStackTrace();
        }



    }
}
