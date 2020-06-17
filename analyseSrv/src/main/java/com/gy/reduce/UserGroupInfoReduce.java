package com.gy.reduce;

import com.gy.entity.UserGroupInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

public class UserGroupInfoReduce implements ReduceFunction<UserGroupInfo> {
    @Override
    public UserGroupInfo reduce(UserGroupInfo value1, UserGroupInfo value2) throws Exception {

        String userid = value1.getUserid();
        List<UserGroupInfo> list1 = value1.getList();
        List<UserGroupInfo> list2 = value2.getList();

        UserGroupInfo userGroupInfoFinal = new UserGroupInfo();
        List<UserGroupInfo> finallist = new ArrayList<UserGroupInfo>();
        finallist.addAll(list1);
        finallist.addAll(list2);
        userGroupInfoFinal.setList(finallist);
        return userGroupInfoFinal;
    }
}
