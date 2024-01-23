package com.airwallex.airskiff.flink;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Clock;
import java.time.Duration;

/**
 * This strategy is determined based on the implementation of
 * org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction
 *
 * <p>We want to catch up quickly at the beginning and be more realtime after we've caught up.
 */
public class RealtimeWatermarkStrategy<T> implements WatermarkStrategy<T> {
  private final Duration maxLateness;

  public RealtimeWatermarkStrategy(Duration maxLateness) {
    assert (!maxLateness.isNegative());
    this.maxLateness = maxLateness;
  }

  @Override
  public WatermarkGenerator<T> createWatermarkGenerator(
    WatermarkGeneratorSupplier.Context context
  ) {
    return new HybridWatermarkGenerator<>(maxLateness.toMillis(), new EventTimeManager(), Clock.systemUTC(), 1000);
  }
}
