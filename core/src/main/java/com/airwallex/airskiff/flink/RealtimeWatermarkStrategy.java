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
  private final Duration allowedLatency;

  public RealtimeWatermarkStrategy(Duration maxLateness, Duration allowedLatency) {
    assert (!maxLateness.isNegative() && !allowedLatency.isNegative());
    this.maxLateness = maxLateness;
    this.allowedLatency = allowedLatency;
  }

  @Override
  public WatermarkGenerator<T> createWatermarkGenerator(
    WatermarkGeneratorSupplier.Context context
  ) {
    return new HybridWatermarkGenerator<>(maxLateness.toMillis(), new EventTimeManager(), Clock.systemUTC(), allowedLatency.toMillis());
  }
}
