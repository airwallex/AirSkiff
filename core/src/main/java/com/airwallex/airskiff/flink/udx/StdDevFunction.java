package com.airwallex.airskiff.flink.udx;

import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.List;

public class StdDevFunction extends AggregateFunction<Double, StdDevAccumulator> {
  @Override
  public Double getValue(StdDevAccumulator acc) {
    List<Double> list = acc.nums.getList();
    if(list.size() == 0 || list.size() == 1) {
      return null;
    } else {
      Double avg = list.stream().mapToDouble(a -> a).average().orElse(0.0);
      Double sum = 0.0;
      for(Double num : list) {
        sum += (num-avg)*(num-avg);
      }
      return Math.sqrt(sum/(list.size()-1));
    }
  }

  @Override
  public StdDevAccumulator createAccumulator() {
    return new StdDevAccumulator();
  }

  public void accumulate(StdDevAccumulator acc, Double value) throws Exception {
    acc.nums.add(value);
  }

  public void retract(StdDevAccumulator acc, Double value) throws Exception {
    acc.nums.remove(value);
  }

  public void resetAccumulator(StdDevAccumulator acc) {
    acc.nums.clear();
  }
}
