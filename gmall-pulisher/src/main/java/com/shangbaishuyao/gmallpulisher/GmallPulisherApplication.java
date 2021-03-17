package com.shangbaishuyao.gmallpulisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan(basePackages = "com.shangbaishuyao.gmallpulisher.mapper")
@SpringBootApplication
public class GmallPulisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(GmallPulisherApplication.class, args);
    }
}
