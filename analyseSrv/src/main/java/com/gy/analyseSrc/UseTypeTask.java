package com.gy.analyseSrc;

import com.gy.entity.UseTypeInfo;
import com.gy.kafka.KafkaEvent;
import com.gy.kafka.KafkaEventSchema;

import com.gy.map.UseTypeMap;
import com.gy.reduce.UseTypeReduce;
import com.gy.reduce.UseTypeSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import javax.annotation.Nullable;

public class UseTypeTask {

    public static void main(String[] args) {

        args = new String [] {"--input-topic","scanProductLog","--bootstrap.servers","hadoop102:9092","--zookeeper.connect","hadoop102:2181","--group.id","amor"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableSysoutLogging();
        environment.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,10000));
        environment.enableCheckpointing(5000);  //create a checkpoint every 5 seconds;
        environment.getConfig().setGlobalJobParameters(parameterTool);    // make paramters avaliable in the web interface
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumerBase<KafkaEvent> kafkaSource = new FlinkKafkaConsumer010<>(parameterTool.getRequired("input-topic"), new KafkaEventSchema(), parameterTool.getProperties()).assignTimestampsAndWatermarks(new UseTypeTask.CustomWatermarkExtractor());
        DataStream<KafkaEvent> input = environment.addSource(kafkaSource);

        DataStream<UseTypeInfo> usetypeMap = input.flatMap(new UseTypeMap());
        DataStream<UseTypeInfo> useTypeReduce = usetypeMap.keyBy("groupbyfield").timeWindowAll(Time.seconds(2)).reduce(new UseTypeReduce());
        useTypeReduce.addSink(new UseTypeSink());

    }

    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent>{

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE :currentTimestamp - 1);
        }

        @Override
        public long extractTimestamp(KafkaEvent element, long previousElementTimestamp) {
            this.currentTimestamp = element.getTimestamp();
            return element.getTimestamp();
        }
    }
}
