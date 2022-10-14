package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.common.functions.SerializableLambda;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class EventTimeAdjustFunction<T> extends RichFlatMapFunction<T, Tuple2<Long, T>> {

  private static final Logger logger = LoggerFactory.getLogger(EventTimeAdjustFunction.class);
  private final SerializableLambda<T, Instant> timeExtractor;
  private long delay = 0;
  private EventTimeManager eventTimeManager;

  public EventTimeAdjustFunction(SerializableLambda<T, Instant> timeExtractor) {
    this.timeExtractor = timeExtractor;
  }

  @Override
  public void open(Configuration parameters) {
    getRuntimeContext().getMetricGroup().gauge("delayBeforeFlink", () -> delay);
    eventTimeManager = new EventTimeManager();
  }

  @Override
  public void flatMap(T t, Collector<Tuple2<Long, T>> out) {
    long currentTime = System.currentTimeMillis();
    long eventTime = timeExtractor.apply(t).toEpochMilli();
    delay = currentTime - eventTime;
    long adjustTime = eventTimeManager.adjust(eventTime, currentTime);
    out.collect(new Tuple2<>(adjustTime, t));
  }
}
