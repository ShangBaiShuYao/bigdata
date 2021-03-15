package com.shangbaishuyao.utils;

import java.io.IOException;
import java.util.Properties;
/**
 * Desc: 通过路径加载配置文件 <br/>
 * create by shangbaishuyao on 2021/3/14
 * @Author: 上白书妖
 * @Date: 18:08 2021/3/14
 */
public class PropertiesUtil {
    public static Properties load(String path) {
        Properties properties = new Properties();
        try {
            properties.load(ClassLoader.getSystemResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
