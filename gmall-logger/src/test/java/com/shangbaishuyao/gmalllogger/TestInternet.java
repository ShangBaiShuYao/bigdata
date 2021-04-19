package com.shangbaishuyao.gmalllogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * 测试网络连通性
 * @author SKY_WEI
 */
public class TestInternet {
    static BufferedReader bufferedReader;
    public static void main(String[] args) throws IOException {
        Scanner input = new Scanner(System.in);
//        System.out.print("请输入IP(180.97.33.107)或者域名(baidu.com):");
//        String address = input.next();
        String address = "192.168.6.102";
        try {
            Process process = Runtime.getRuntime().exec("ping " + address + " -t");
            bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String connectionStr = null;
            while ((connectionStr = bufferedReader.readLine()) != null) {
                System.out.println(connectionStr);
            }
        } catch (IOException e) {
            System.out.println("链接失败");
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }
    }
}