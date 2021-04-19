package com.shangbaishuyao.gmalllogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TestSocket {
    public static void main(String[] args) throws IOException {
        StringBuilder sb1 = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            Process p = Runtime.getRuntime().exec("ping " + "192.168.16."+ i);
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            StringBuilder sb = new StringBuilder();
            while((line = br.readLine())!=null) {
                sb.append(line);
            }
            if(null != sb && !sb.toString().equals("")) {
                String longString = "";
                if(sb.toString().indexOf("TTL")>0) {
                    sb1.append("ping " + "192.168.16."+ i);
                }
                else {
                    System.out.println("ping " + "192.168.16."+ i +"无法ping通");
                }
            }
        }
        System.out.println("以下可以ping通");
        System.out.println(sb1.toString());
    }
}
