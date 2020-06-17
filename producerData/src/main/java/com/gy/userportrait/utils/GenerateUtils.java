package com.gy.userportrait.utils;

import com.gy.userportrait.entities.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateUtils {

    static List<User> list = new ArrayList<>();
    static String [] emailArray = {"@163.com", "@126.com","@139.com","@sohu.com","@qq.com","@189.cn","@tom.com","@aliyum.com","@sina.com"};
    static int telArray [] = {133,153,180,181,189,177,1700,173,199,130,131,132,155,156,185,186,145,176,1709,134,135,136,137,138,139,150,151,152,157,158,159,182,183,184,187,188,147,178,1705};
    static String [] nameArray = {"赵","钱","孙","李","周","吴","郑","王","范","陈","高","齐","司马","曹"};
    static Random random = new Random();

    public static String generateEmail(){
        StringBuffer sb = new StringBuffer();
        for(int i=0; i< 10;i++){
            int i1 = random.nextInt(10);
            sb.append(i1);
        }
        sb.append(emailArray[random.nextInt(emailArray.length)]);
        return sb.toString();
    }


    public static String generateTelephone(){
        StringBuffer sb = new StringBuffer();
        sb.append(telArray[random.nextInt(telArray.length)]);

        while(sb.toString().length() <11){
            int i1 = random.nextInt(10);
            sb.append(i1);
        }

        int carrierByTel = CarrierUtils.getCarrierByTel(sb.toString());
        System.out.println(carrierByTel);


        return sb.toString();
    }

    public static String generateRegisterTime(){

        int [] day1 = {1,2,3,4,5,6,7,8,9,0};
        int [] day2 = {0,1,2,3};
        int [] month = {1,2,3,4,5,6,7,8,9,10,11,12};
        int []  year1 = {19,20};
        int [] yearend = {0,1,2,3,4,5,6,7,8,9};

        StringBuffer sb = new StringBuffer();

        sb.append(year1[random.nextInt(2)]);
        sb.append(yearend[random.nextInt(yearend.length)]);
        sb.append(yearend[random.nextInt(yearend.length)]);
        sb.append("-");
        int monthTemp =month[random.nextInt(month.length)];
        sb.append(monthTemp);
        int i = day2[random.nextInt(day2.length)];
        if(i < 3){
            sb.append("-").append(i).append(day1[random.nextInt(day1.length)]);
        }else{
            sb.append("-").append(i).append("0");
        }

        return sb.toString();
    }

    public static String getName(){
        String [] nameArray1 = {"一","天","炳","月","岚","杰","毅","空","天","战","武","文","白","轩"};
        String [] nameArray2 = {"海","泉","勇","湉","起","浅","三","茜","玉","漱","虞","姬","嘉","珺"};

        StringBuffer sb = new StringBuffer();

        sb.append(nameArray[random.nextInt(nameArray.length)]);
        sb.append(nameArray1[random.nextInt(nameArray1.length)]);
        sb.append(nameArray2[random.nextInt(nameArray2.length)]);

        return sb.toString();
    }

    public static void upload(String path) throws Exception {
        //1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "amor");

        //2.获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File(path));
        //3.获取输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/userportrait/user"));

        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);

        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        fileSystem.close();
    }



    public static String getIp(){
        StringBuffer sb = new StringBuffer();

        for(int i=0;i<4;i++){
            sb.append(random.nextInt(255)).append(".");
        }

        return  sb.toString().substring(0, sb.toString().length()-1);
    }


    public static void main(String[] args) {

    }
}
