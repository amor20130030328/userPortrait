package com.gy.userportrait;

import com.gy.userportrait.entities.ScanProductLog;
import com.gy.userportrait.utils.GenerateUtils;
import org.apache.kafka.clients.producer.*;

import java.util.*;

public class ScanProductLogProducer {


    public static void main(String[] args) {

        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092");
        // 等待所有副本节点的应答
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 消息发送最大尝试次数
        props.put("retries", 3);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        List<String> brand = Arrays.asList("李宁", "爱迪达斯", "森马", "海尔");

        ScanProductLog popularManAndWoman = null;
        Random random = new Random();



        while (true) {

                popularManAndWoman = new ScanProductLog();
                popularManAndWoman.setUserid(random.nextInt(100000));
                popularManAndWoman.setUsetype(random.nextInt(2));
                popularManAndWoman.setProductid(random.nextInt(19) + 1);
                popularManAndWoman.setProducttypeid(random.nextInt(14) + 1);
                popularManAndWoman.setBrand(brand.get(random.nextInt(brand.size())));
                popularManAndWoman.setIp(GenerateUtils.getIp());
                popularManAndWoman.setScantime(GenerateUtils.generateRegisterTime());
                popularManAndWoman.setStaytime(GenerateUtils.generateRegisterTime());


                producer.send(new ProducerRecord<>("scanProductLog", popularManAndWoman.toString()), new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {

                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        if (exception == null) {
                            System.err.println(metadata.topic() + " " + metadata.offset() + " " + metadata.partition());
                        } else {
                            System.err.println("发送失败。。。");
                        }
                    }
                });


        }

    }

}
