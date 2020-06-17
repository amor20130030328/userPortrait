package com.gy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class HBaseUtils {

    private static Admin admin = null;
    private static Connection conn = null;
    static {
        //创建hbase 配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop102:9000/HBase");
        //使用eclipses时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.quorum","hadoop102");
        conf.set("hbase.client.scanner.timeout.period","600000");
        conf.set("hbase.rpc.timeout","600000");

        try{
            conn = ConnectionFactory.createConnection(conf);
            //得到管理程序
            admin = conn.getAdmin();
        }catch (Exception e){

        }
    }

    /**
     * 插入数据 ，create "baseuserscaninfo","time"
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param datamap
     * @throws Exception
     */
    public static void putData(String tableName, String rowKey, String familyName, Map<String,String> datamap) throws Exception{

        Table table = conn.getTable(TableName.valueOf(tableName));
        //将字符串转换成 byte[]
        byte[] rowKeyByte = Bytes.toBytes(rowKey);
        Put put = new Put(rowKeyByte);
        if(datamap != null){
            Set<Map.Entry<String, String>> set = datamap.entrySet();
            for(Map.Entry<String,String> entry : set){
                String key = entry.getKey();
                String value = entry.getValue();
                put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(key),Bytes.toBytes(value + ""));
            }
        }

        table.put(put);
        table.close();
        System.out.println("HBASE INSERT OK !!!");
    }

    /**
     * 获取数据 create "baseuserscaninfo" ,"time"
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @return
     * @throws IOException
     */
    public static String getDate(String tableName,String rowKey, String familyName,String column) throws IOException {

        Table table = conn.getTable(TableName.valueOf(tableName));
        //将字符串转换成 byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowKey);
        Get get = new Get(rowkeybyte);
        Result result = table.get(get);
        byte[] resultbytes = result.getValue(familyName.getBytes(), column.getBytes());
        if(resultbytes == null){
            return null;
        }
        return new String(resultbytes);
    }

    /**
     * 插入数据   create "baseuserscaninfo","time"
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @param data
     * @throws Exception
     */
    public static void putData(String tableName , String rowKey, String familyName , String column , String data) throws Exception{

        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(),column.getBytes(),data.getBytes());
        table.put(put);
    }





}
