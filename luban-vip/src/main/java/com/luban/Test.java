package com.luban;

import org.apache.zookeeper.server.persistence.Util;

import java.time.LocalDate;
import java.util.Locale;

public class Test {
    public static void main(String[] args) {
//        System.out.println(String.format(Locale.ENGLISH, "%010d", 1));
        System.out.println(Util.makeLogName(100));

        LocalDate now = LocalDate.now();
        System.out.println(now.getMonthValue());


        if(true){
            System.out.println("if");
        }else if(true){
            System.out.println("else if");
        }

    }
}
