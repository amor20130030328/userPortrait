package com.gy.map;

import com.gy.entity.EmailInfo;
import com.gy.util.EmailUtils;
import com.gy.util.HBaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class EmailMap  implements MapFunction<String,EmailInfo> {
    @Override
    public EmailInfo map(String s) throws Exception {

        if(StringUtils.isBlank(s)){
            return null;
        }

        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String username = userinfos[1];
        String sex = userinfos[2];
        String telphone = userinfos[3];
        String email = userinfos[4];
        String age = userinfos[5];
        String registerTime = userinfos[6];
        String usetype = userinfos[7];    //终端类型 : 0.pc端  1.移动端  2.小程序端

        String emailType = EmailUtils.getEmailTypeBy(email);
        String tableName = "userflaginfo";
        String rowkey = userid;
        String familyName = "baseinfo";
        String column = "emailinfo";

        HBaseUtils.putData(tableName,rowkey,familyName,column,emailType);
        EmailInfo emailInfo = new EmailInfo();

        String groupField = "emailInfo==" + emailType;
        emailInfo.setEmailType(emailType);
        emailInfo.setCount(1L);
        emailInfo.setGroupField(groupField);
        return emailInfo;
    }
}
