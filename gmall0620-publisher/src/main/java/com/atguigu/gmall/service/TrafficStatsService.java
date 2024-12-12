package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TrafficUvCt;

import java.util.List;

/**
 * @author Felix
 * @date 2024/12/12
 * 流量域统计service接口
 */
public interface TrafficStatsService {
    //获取某天各个渠道独立访客
    List<TrafficUvCt> getChUvCt(Integer date,Integer limit);
}
