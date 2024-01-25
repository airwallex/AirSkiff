package com.airwallex.airskiff.flink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.time.Clock;

public class HybridWatermarkGenerator<T> implements WatermarkGenerator<T> {
  private final long maxDelay;
  private final EventTimeManager eventTimeManager;
  private final Clock clock;
  private long maxTs;
  private long lastProcessTime;
  private long allowedLatency;

  public HybridWatermarkGenerator(long maxDelay, EventTimeManager eventTimeManager, Clock clock) {
    this.maxDelay = maxDelay;
    this.eventTimeManager = eventTimeManager;
    this.clock = clock;
    this.lastProcessTime = clock.millis();
    this.allowedLatency = 0;
  }

  public HybridWatermarkGenerator(long maxDelay, EventTimeManager eventTimeManager, Clock clock, long allowedLatency) {
    this.maxDelay = maxDelay;
    this.eventTimeManager = eventTimeManager;
    this.clock = clock;
    this.lastProcessTime = clock.millis();
    this.allowedLatency = allowedLatency;
  }

  @Override
  public void onEvent(T t, long ts, WatermarkOutput watermarkOutput) {
    maxTs = Math.max(maxTs, ts);
    long currentTime = clock.millis();
    lastProcessTime = currentTime;
    eventTimeManager.checkCaughtUp(currentTime - ts);
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
    if (eventTimeManager.isCaughtUp()) {
      watermarkOutput.emitWatermark(new Watermark(maxTs - allowedLatency));
    } else {
      long elapsed = clock.millis() - lastProcessTime;
      if (elapsed >= Constants.TEN_SECONDS.toMillis()) {
        watermarkOutput.emitWatermark(new Watermark(maxTs - allowedLatency));
      } else {
        watermarkOutput.emitWatermark(new Watermark(maxTs - maxDelay));
      }
    }
  }
}
