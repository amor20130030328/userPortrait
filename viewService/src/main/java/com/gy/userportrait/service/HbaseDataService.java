package com.gy.userportrait.service;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;

@FeignClient(value = "searchInfo")
public interface HbaseDataService {

    @PostMapping(value ="/hbaseData/baiJiaZhiShuInfo")
    String baiJiaZhiShuInfo(String userId);

    @PostMapping(value = "hbaseData/brandLike")
    String brandLike(String userid);

    @PostMapping(value = "hbaseData/carrierinfo")
    String carrierinfo(String userid);

    @PostMapping(value = "hbaseData/popularManAndWomen")
    String chaomanandwomen(String userid);

    @PostMapping(value = "hbaseData/consumptionlevel")
    String consumptionlevel(String userid);

    @PostMapping(value = "hbaseData/emailinfo")
    String emailinfo(String userid);

    @PostMapping(value = "hbaseData/yearkeyword")
    String yearkeyword(String userid);

    @PostMapping(value = "hbaseData/monthkeyword")
    String monthkeyword(String userid);

    @PostMapping(value = "hbaseData/quarterkeyword")
    String quarterkeyword(String userid);

    @PostMapping(value = "hbaseData/sex")
    String sex(String userid);

    @PostMapping(value = "hbaseData/usergroupinfo")
    String usergroupinfo(String userid);

    @PostMapping(value = "hbaseData/usetypeinfo")
    String usetypeinfo(String userid);

    @PostMapping(value = "hbaseData/ageinfo")
    String ageinfo(String userid);

}
