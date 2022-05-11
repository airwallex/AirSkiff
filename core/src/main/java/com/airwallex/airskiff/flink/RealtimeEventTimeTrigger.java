package com.airwallex.airskiff.flink;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Triggers on each event only
 */
public class RealtimeEventTimeTrigger extends Trigger<Object, TimeWindow> {
  private RealtimeEventTimeTrigger() {
  }

  public static RealtimeEventTimeTrigger create() {
    return new RealtimeEventTimeTrigger();
  }

  @Override
  public TriggerResult onElement(
    Object element, long timestamp, TimeWindow window, TriggerContext ctx
  ) throws Exception {
    return TriggerResult.FIRE;
  }

  @Override
  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

  @Override
  public String toString() {
    return "RealtimeEventTimeTrigger()";
  }
}
