package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Felix
 * @date 2024/12/07
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T,String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        String jsonStr = JSON.toJSONString(bean, config);
        return jsonStr;
    }
}
