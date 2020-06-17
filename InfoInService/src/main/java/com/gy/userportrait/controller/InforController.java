package com.gy.userportrait.controller;

import com.alibaba.fastjson.JSONObject;
import com.gy.userportrait.entity.ResultMessage;
import com.youfan.log.AttentionProductLog;
import com.youfan.log.BuyCartProductLog;
import com.youfan.log.CollectProductLog;
import com.youfan.log.ScanProductLog;
import com.youfan.utils.ReadProperties;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

@RestController
public class InforController {

    private final String attentionProductLogTopic = ReadProperties.getKey("attentionProductLog","search.properties");
    private final String buyCartProductLogTopic = ReadProperties.getKey("buyCartProductLog","search.properties");
    private final String collectProductLogTopic = ReadProperties.getKey("collectProductLog","search.properties");
    private final String scanProductLogTopic = ReadProperties.getKey("scanProductLog","search.properties");


    @Autowired
    private KafkaTemplate kafkaTemplate;

    @GetMapping("/helloworld")
    String helloWorld(HttpServletRequest request){
        String ip = request.getRemoteAddr();
        ResultMessage resultMessage = new ResultMessage();
        resultMessage.setMessage("hello:" + ip);
        resultMessage.setStatus("success");
        String result = JSONObject.toJSONString(resultMessage);
        return  result;
    }


    @PostMapping("/receivelog")
    String helloworld(String receivelog,HttpServletRequest request){

        if(StringUtils.isBlank(receivelog)){
            return null;
        }


        String [] rearrays = receivelog.split(":",2);
        String classname = rearrays[0];
        String data = rearrays[1];
        String resultmessage = "";

        if("AttentionProductLog".equals(classname)){
            AttentionProductLog attentionProductLog = JSONObject.parseObject(data, AttentionProductLog.class);
            resultmessage  = JSONObject.toJSONString(attentionProductLog);
            kafkaTemplate.send(attentionProductLogTopic,resultmessage + "##1##" + new Date().getTime());
        }else  if("BuyCartProductLog".equals(classname)){
            BuyCartProductLog buyCartProductLog = JSONObject.parseObject(data, BuyCartProductLog.class);
            resultmessage  = JSONObject.toJSONString(buyCartProductLog);
            kafkaTemplate.send(buyCartProductLogTopic,resultmessage + "##1##" + new Date().getTime());
        }else  if("CollectProductLog".equals(classname)){
            CollectProductLog collectProductLog = JSONObject.parseObject(data, CollectProductLog.class);
            resultmessage  = JSONObject.toJSONString(collectProductLog);
            kafkaTemplate.send(collectProductLogTopic,resultmessage + "##1##" + new Date().getTime());
        }else  if("ScanProductLog".equals(classname)){
            ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
            resultmessage  = JSONObject.toJSONString(scanProductLog);
            kafkaTemplate.send(scanProductLogTopic,resultmessage + "##1##" + new Date().getTime());
        }

        ResultMessage resultMessage = new ResultMessage();
        resultMessage.setMessage(resultmessage);
        resultMessage.setStatus("success");
        String result = JSONObject.toJSONString(resultMessage);
        return result;
    }

}
