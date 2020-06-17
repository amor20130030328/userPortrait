package com.gy.userportrait.controller;

import com.alibaba.fastjson.JSONObject;
import com.gy.userportrait.form.AnalyForm;
import com.gy.userportrait.service.HbaseDataService;
import com.youfan.entity.ViewResultAnaly;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/hbaseData")
@CrossOrigin
public class HbaseDataViewControl {


    //    hbaseData/resultinfoView
    @Autowired
    private HbaseDataService hbaseDataService;

    @GetMapping("test")
    public String testFeign(@RequestParam("userId") String userId){
        return hbaseDataService.baiJiaZhiShuInfo(userId);
    }

    @PostMapping(value = "resultinfoView",produces = "application/json;charset=UTF-8")
    public String resultInfoView(@RequestBody AnalyForm analyForm){
        String type = analyForm.getType();
        String userId = analyForm.getUserId();
        String result = "";
        List<ViewResultAnaly> resultList = new ArrayList<>();
        if("-1".equals(type)){
            ViewResultAnaly viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.baiJiaZhiShuInfo(userId);
            viewResultAnaly.setTypename("败家指数");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);

            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.brandLike(userId);
            viewResultAnaly.setTypename("品牌偏好");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.carrierinfo(userId);
            viewResultAnaly.setTypename("运营商");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.chaomanandwomen(userId);
            viewResultAnaly.setTypename("潮男潮女");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.consumptionlevel(userId);
            viewResultAnaly.setTypename("消费水平");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.emailinfo(userId);
            viewResultAnaly.setTypename("邮件运营商");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.yearkeyword(userId);
            viewResultAnaly.setTypename("年度关键词");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.sex(userId);
            viewResultAnaly.setTypename("性别");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.usergroupinfo(userId);
            viewResultAnaly.setTypename("用户群体特征");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.usetypeinfo(userId);
            viewResultAnaly.setTypename("终端偏好");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            result = hbaseDataService.ageinfo(userId);
            viewResultAnaly.setTypename("年龄");
            viewResultAnaly.setLablevalue(result);
            resultList.add(viewResultAnaly);
            viewResultAnaly = new ViewResultAnaly();
            viewResultAnaly.setList(resultList);
            String resultjson = JSONObject.toJSONString(viewResultAnaly);
            return resultjson;
        }

        if("baiJiaZhiShuInfo".equals(type)){
            result = hbaseDataService.baiJiaZhiShuInfo(userId);
        }else if ("brandLike".equals(type)){
            result = hbaseDataService.brandLike(userId);
        }else if ("carrierinfo".equals(type)){
            result = hbaseDataService.carrierinfo(userId);
        }else if ("chaomanandwomen".equals(type)){
            result = hbaseDataService.chaomanandwomen(userId);
        }else if ("consumptionlevel".equals(type)){
            result = hbaseDataService.consumptionlevel(userId);
        }else if ("emailinfo".equals(type)){
            result = hbaseDataService.emailinfo(userId);
        }else if ("yearkeyword".equals(type)){
            result = hbaseDataService.yearkeyword(userId);
        }else if ("monthkeyword".equals(type)){
            result = hbaseDataService.monthkeyword(userId);
        }else if ("quarterkeyword".equals(type)){
            result = hbaseDataService.quarterkeyword(userId);
        }else if ("sex".equals(type)){
            result = hbaseDataService.sex(userId);
        }else if ("usergroupinfo".equals(type)){
            result = hbaseDataService.usergroupinfo(userId);
        }else if ("usetypeinfo".equals(type)){
            result = hbaseDataService.usetypeinfo(userId);
        }else if ("ageinfo".equals(type)){
            result = hbaseDataService.ageinfo(userId);
        }
        ViewResultAnaly viewResultAnaly = new ViewResultAnaly();
        viewResultAnaly.setResult(result);
        result = JSONObject.toJSONString(viewResultAnaly);
        return result;
    }



}
