package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.mapper.TradeStatsMapper;
import com.atguigu.gmall.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2024/12/11
 * 交易域统计service层实现
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {

    @Autowired
    TradeStatsMapper tradeStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
