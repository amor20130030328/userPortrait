package com.gy.userportrait.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HbaseServiceImpl {

    private static Admin admin = null;
    private static Connection connection = null;

    static {
        //创建hbase 配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://hadoop102:9000/HBase");
        //使用eclipse 时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.client.scanner.timeout.period", "600000");
        conf.set("hbase.rpc.timeout", "600000");

        try {
            connection = ConnectionFactory.createConnection(conf);
            //得到管理程序
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getData(String tableName,String rowkey,String familyName,String column) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        //将字符串转换成 byte []
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result = table.get(get);
        byte[] resultbytes = result.getValue(familyName.getBytes(), column.getBytes());
        if (resultbytes == null){
            return null;
        }
        return new String(resultbytes);
    }
}
