package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.mapper.TrafficStatsMapper;
import com.atguigu.gmall.service.TrafficStatsService;

import java.util.List;

/**
 * @author Felix
 * @date 2024/12/12
 * 流量域统计service接口实现
 */
public class TrafficStatsServiceImpl  implements TrafficStatsService {
    TrafficStatsMapper trafficStatsMapper;
    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date,limit);
    }
}
