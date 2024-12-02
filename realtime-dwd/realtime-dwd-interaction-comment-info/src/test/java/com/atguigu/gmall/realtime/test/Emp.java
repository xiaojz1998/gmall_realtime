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
public class Emp {
    Integer empno;
    String ename;
    Integer deptno;
    Long ts;
}
