package com.shangbaishuyao.gmallpulisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);

}