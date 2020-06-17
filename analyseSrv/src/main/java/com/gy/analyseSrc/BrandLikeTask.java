package com.gy.analyseSrc;

import com.gy.entity.BrandLike;
import com.gy.kafka.KafkaEvent;
import com.gy.kafka.KafkaEventSchema;
import com.gy.map.BrandLikeMap;
import com.gy.reduce.BrandLikeReduce;
import com.gy.reduce.BrandLikeSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import javax.annotation.Nullable;

public class BrandLikeTask {

    public static void main(String[] args) {

            args = new String[]{"--input-topic","scanProductLog","--bootstrap.servers","hadoop102:9092","--zookeeper.connect","hadoop102:2181","--group.id","amor"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableSysoutLogging();
        environment.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,10000));
        environment.enableCheckpointing(5000);    //create a checkpoint every 5 seconds
        environment.getConfig().setGlobalJobParameters(parameterTool); //make parameters available in the web interface
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        FlinkKafkaConsumerBase<KafkaEvent> kafkaSource = new FlinkKafkaConsumer010<>(parameterTool.getRequired("input-topic"), new KafkaEventSchema(), parameterTool.getProperties()).assignTimestampsAndWatermarks(new CustomerWatermarkExtractor());
        DataStream<KafkaEvent> input = environment.addSource(kafkaSource);
        DataStream<BrandLike> brandLikeMap = input.flatMap(new BrandLikeMap());
        DataStream<BrandLike> brandLikeReduce = brandLikeMap.keyBy("groupbyfield").timeWindowAll(Time.seconds(2)).reduce(new BrandLikeReduce());
        brandLikeReduce.addSink(new BrandLikeSink());

        try {
            environment.execute("brandlike analy");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static class CustomerWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent>{

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }

        @Override
        public long extractTimestamp(KafkaEvent element, long previousElementTimestamp) {
            this.currentTimestamp = element.getTimestamp();
            return element.getTimestamp();
        }
    }

}


