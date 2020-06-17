package com.gy.userportrait.service;

import com.youfan.entity.AnalyResult;
import jdk.internal.dynalink.linker.LinkerServices;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.List;

@FeignClient(value = "searchInfo")
public interface MongoDataService {

    @PostMapping(value = "/yearBase/searchYearBase")
    List<AnalyResult> searchYearBase();


    @PostMapping(value = "/yearBase/searchUseType")
    List<AnalyResult> searchUseType();

    @PostMapping(value = "/yearBase/searchEmail")
    List<AnalyResult> searchYearEmail();

    @PostMapping(value = "/yearBase/searchConsumptionLevel")
    List<AnalyResult> searchConsumptionLevel();

    @PostMapping(value = "/yearBase/searchPopularManAndWoman")
    List<AnalyResult> searchPopularManAndWoman();

    @PostMapping(value = "/yearBase/searchCarrier")
    List<AnalyResult> searchCarrier();

    @PostMapping(value = "/yearBase/searchBrandLike")
    List<AnalyResult> searchBrandLike();

}
