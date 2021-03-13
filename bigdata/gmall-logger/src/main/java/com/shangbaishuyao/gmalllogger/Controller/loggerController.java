package com.shangbaishuyao.gmalllogger.Controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
/**
 * Desc: 测试打印日志数据 <br/>
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 21:37 2021/3/13
 */
//@Controller+@ResponseBody = @RestController
@RestController
public class loggerController {
    @PostMapping("log")
    public String logger(@RequestParam("logString")String logString){
        System.out.println(logString);
        return "success";
    }
}
