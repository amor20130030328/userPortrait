package com.gy.kmeans;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KmeansFinalMap implements MapFunction<String,Point> {

    private List<Point> centers = new ArrayList<>();

    public KmeansFinalMap(List<Point> finalClusterCenter) {
        this.centers = finalClusterCenter;
    }


    @Override
    public Point map(String value) throws Exception {

        if(StringUtils.isBlank(value)){
            return null;
        }

        Random random = new Random();
        String[] temps = value.split(",");
        String variable1 = temps[0];
        String variable2 = temps[1];
        String variable3 = temps[2];
        Point self = new Point(1,new float[]{Float.valueOf(variable1),Float.valueOf(variable2),Float.valueOf(variable3)});
        float min_dis = Integer.MAX_VALUE;
        for(Point point : centers){
               float tmp_dis = (float) Math.min(DistanceCompute.getEuclideanDis(self,point),min_dis);
               min_dis = tmp_dis;
               self.setClusterId(point.getId());
               self.setDist(min_dis);
               self.setClusterPoint(point);
        }

        return self;
    }
}
