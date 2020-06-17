package com.gy.userportrait.controller;


import com.alibaba.fastjson.JSONObject;
import com.gy.userportrait.form.AnalyForm;
import com.gy.userportrait.service.MongoDataService;
import com.youfan.entity.AnalyResult;
import com.youfan.entity.ViewResultAnaly;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/mongoData")
@CrossOrigin
public class MongoDataViewControl {

    @Resource
    MongoDataService mongoDataService;

    @PostMapping(value = "resultInfoView",produces = "application/json;charset=UTF-8")
    public String resultInfoView(@RequestBody AnalyForm analyForm){

        String type = analyForm.getType();
        List<AnalyResult> list = new ArrayList<>();
        if("yearBase".equals(type)){
            list = mongoDataService.searchYearBase();
        }else if("useType".equals(type)){
            list = mongoDataService.searchUseType();
        }else if("email".equals(type)){
            list = mongoDataService.searchYearEmail();
        }else if("consumptionLevel".equals(type)){
            list = mongoDataService.searchConsumptionLevel();
        }else if("carrier".equals(type)){
            list = mongoDataService.searchCarrier();
        }else if("popularManAndWoman".equals(type)){
            list = mongoDataService.searchPopularManAndWoman();
        }else if("brandLike".equals(type)){
            list = mongoDataService.searchBrandLike();
        }

        ViewResultAnaly viewResultAnaly = new ViewResultAnaly();
        List<String> infoList = new ArrayList<>();     //分组list， x 轴的值
        List<Long> countList = new ArrayList<>();     //数量

        for(AnalyResult analyResult : list){
            infoList.add(analyResult.getInfo());
            countList.add(analyResult.getCount());
        }

        viewResultAnaly.setInfolist(infoList);
        viewResultAnaly.setCountlist(countList);
        String result = JSONObject.toJSONString(viewResultAnaly);
        return result;
    }
}
