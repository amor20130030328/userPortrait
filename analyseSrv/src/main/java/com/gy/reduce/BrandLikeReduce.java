package com.gy.reduce;

import com.gy.entity.BrandLike;
import org.apache.flink.api.common.functions.ReduceFunction;

public class BrandLikeReduce implements ReduceFunction<BrandLike> {
    @Override
    public BrandLike reduce(BrandLike brandLike, BrandLike brandLike2) throws Exception {
        String brand = brandLike.getBrand();
        long count1 = brandLike.getCount();
        long count2 = brandLike2.getCount();
        BrandLike brandLikeFinal = new BrandLike();
        brandLikeFinal.setBrand(brand);
        brandLikeFinal.setCount(count1 + count2);
        return brandLikeFinal;
    }
}
