//package com.shangbaishuyao;
//
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.BufferedReader;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//
//public class NetworkState {
//    private static Logger log = LoggerFactory.getLogger(NetworkState.class);
//    /**
//     * test network
//     * @param ip
//     */
//    private void getNetworkState(String ip) {
//        Runtime runtime = Runtime.getRuntime();
//        try {
//            log.info("=================正在测试网络连通性ip："+ip);
//            Process process = runtime.exec("ping " +ip);
//            InputStream iStream = process.getInputStream();
//            InputStreamReader iSReader = new InputStreamReader(iStream,"UTF-8");
//            BufferedReader bReader = new BufferedReader(iSReader);
//            String line = null;
//            StringBuffer sb = new StringBuffer();
//            while ((line = bReader.readLine()) != null) {
//                sb.append(line);
//            }
//            iStream.close();
//            iSReader.close();
//            bReader.close();
//            String result = new String(sb.toString().getBytes("UTF-8"));
//            log.info("ping result:"+result);
//            if (!StringUtils.isBlank(result)) {
//                if (result.indexOf("TTL") > 0 || result.indexOf("ttl") > 0) {
//                    log.info("网络正常，时间: " + TimeUtil.getCurDate("yyyy-mm-dd hh:mm:ss"));
//                } else {
//                    log.info("网络断开，时间 :" + TimeUtil.getCurDate("yyyy-mm-dd hh:mm:ss"));
//
//                }
//            }
//        } catch (Exception e) {
//            log.error("网络异常："+e.getMessage());
//            e.printStackTrace();
//        }
//    }
//}
