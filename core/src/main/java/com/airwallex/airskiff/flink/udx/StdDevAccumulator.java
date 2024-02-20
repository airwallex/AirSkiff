package com.airwallex.airskiff.flink.udx;

import org.apache.flink.table.api.dataview.ListView;

public class StdDevAccumulator {
  public ListView<Double> nums = new ListView<>();
}
