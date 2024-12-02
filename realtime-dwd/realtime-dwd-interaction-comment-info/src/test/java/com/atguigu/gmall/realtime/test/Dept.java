package com.atguigu.gmall.realtime.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2024/12/02
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    Integer deptno;
    String dname;
    Long ts;
}
