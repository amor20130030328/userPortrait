package com.gy.analyseSrc;

import com.gy.entity.EmailInfo;
import com.gy.map.EmailMap;
import com.gy.reduce.EmailReduce;
import com.gy.util.MongoUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import scala.xml.PrettyPrinter;

import java.util.List;

public class EmailTask {

    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<EmailInfo> mapReduce = text.map(new EmailMap());

        DataSet<EmailInfo> reduceResult = mapReduce.groupBy("groupField").reduce(new EmailReduce());

        try {
            List<EmailInfo> resultList = reduceResult.collect();
            for (EmailInfo emailInfo: resultList) {
                String emailType = emailInfo.getEmailType();
                Long count = emailInfo.getCount();

                System.out.println(emailType + "===" + count);

                Document doc = MongoUtil.findOneBy("emailstatics", "portrait", emailType);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",emailType);
                    doc.put("count",count);
                }else{
                    Long countPre = doc.getLong("count");
                    Long total = countPre + count;

                    doc.put("count",total);

                }
                MongoUtil.saveOrUpdateMongo("emailstatics","portrait",doc);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
