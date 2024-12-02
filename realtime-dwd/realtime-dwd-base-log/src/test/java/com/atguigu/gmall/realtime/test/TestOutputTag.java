package com.atguigu.gmall.realtime.test;

import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2024/12/02
 * 该案例演示了侧输出流标签的创建
 */
public class TestOutputTag {
    public static void main(String[] args) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
    }
}
