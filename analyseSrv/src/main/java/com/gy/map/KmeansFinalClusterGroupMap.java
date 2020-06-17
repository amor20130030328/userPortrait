package com.gy.map;

import com.alibaba.fastjson.JSONObject;
import com.gy.entity.UserGroupInfo;
import com.gy.kmeans.DistanceCompute;
import com.gy.kmeans.Point;
import com.gy.util.HBaseUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class KmeansFinalClusterGroupMap implements MapFunction<UserGroupInfo,Point> {
    private List<Point> centers = new ArrayList<Point>();

    public KmeansFinalClusterGroupMap(List<Point> centers) {
        this.centers = centers;
    }

    @Override
    public Point map(UserGroupInfo userGroupInfo) throws Exception {

        float[] f = new float[]{Float.valueOf(userGroupInfo.getUserid()+""),Float.valueOf(userGroupInfo.getAvramount()+""),Float.valueOf(userGroupInfo.getMaxamount()+""),Float.valueOf(userGroupInfo.getDays()),
                Float.valueOf(userGroupInfo.getBuytype1()),Float.valueOf(userGroupInfo.getBuytype2()),Float.valueOf(userGroupInfo.getBuytype3()),
                Float.valueOf(userGroupInfo.getBuytime1()),Float.valueOf(userGroupInfo.getBuytime2()),Float.valueOf(userGroupInfo.getBuytime3()),
                Float.valueOf(userGroupInfo.getBuytime4())};
        Point self = new Point(Integer.valueOf(userGroupInfo.getUserid()),f);
        float min_dis = Integer.MAX_VALUE;
        for(Point point : centers){
            float tmp_dis = (float) Math.min(DistanceCompute.getEuclideanDis(self, point), min_dis);
            if (tmp_dis != min_dis) {
                min_dis = tmp_dis;
                self.setClusterId(point.getId());
                self.setDist(min_dis);
                self.setClusterPoint(point);
            }
        }
        String tablename = "userflaginfo";
        String rowkey = self.getId()+"";
        String famliyname = "usergroupinfo";
        String colum = "usergroupinfo";//用户分群信息
        HBaseUtils.putData(tablename,rowkey,famliyname,colum, JSONObject.toJSONString(self));

        return self;
    }
}
