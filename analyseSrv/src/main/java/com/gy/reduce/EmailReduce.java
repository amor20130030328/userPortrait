package com.gy.reduce;

import com.gy.entity.EmailInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

public class EmailReduce implements ReduceFunction<EmailInfo> {
    @Override
    public EmailInfo reduce(EmailInfo emailInfo, EmailInfo t1) throws Exception {

        String emailType = emailInfo.getEmailType();
        Long count1 = emailInfo.getCount();
        Long count2 = t1.getCount();
        EmailInfo emailInfoFinal = new EmailInfo();
        emailInfoFinal.setCount( count1 + count2 );
        emailInfoFinal.setEmailType(emailType);
        emailInfoFinal.setGroupField(emailInfo.getGroupField());
        return emailInfoFinal;
    }
}
