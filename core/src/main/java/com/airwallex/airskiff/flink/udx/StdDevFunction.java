package com.airwallex.airskiff.flink.udx;

import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.List;

public class StdDevFunction extends AggregateFunction<Double, StdDevAccumulator> {
  @Override
  public Double getValue(StdDevAccumulator acc) {
    if(acc.count == 0 || acc.count == 1) {
      return null;
    }
    List<Double> list = acc.nums.getList();
    double avg = acc.sum / acc.count;
    double squaredDiffSum = 0.0;
    for(double num : list) {
      squaredDiffSum  += Math.pow(num - avg, 2);
    }
    return Math.sqrt(squaredDiffSum/(acc.count-1));
  }

  @Override
  public StdDevAccumulator createAccumulator() {
    return new StdDevAccumulator();
  }

  public void accumulate(StdDevAccumulator acc, Double value) throws Exception {
    acc.count++;
    acc.sum += value;
    acc.nums.add(value);
  }

  public void retract(StdDevAccumulator acc, Double value) throws Exception {
    acc.count--;
    acc.sum -= value;
    acc.nums.remove(value);
  }

  public void resetAccumulator(StdDevAccumulator acc) {
    acc.count = 0;
    acc.sum = 0.0;
    acc.nums.clear();
  }
}
