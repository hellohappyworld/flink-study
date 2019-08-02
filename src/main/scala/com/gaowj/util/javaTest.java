package com.gaowj.util;

import java.util.List;

public class javaTest {
    public static void main(String[] args) {
        List<String> list = FindHdfsPaths.existFiles("/user/flink/backup_file/appsta/2019-07-22--1427/*");

        String[] arr = list.toArray(new String[list.size()]);
        for (int i=0;i<arr.length;i++){
            System.out.println(arr[i]);
        }
    }
}
