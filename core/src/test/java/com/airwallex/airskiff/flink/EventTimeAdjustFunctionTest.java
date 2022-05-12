package com.airwallex.airskiff.flink;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class EventTimeAdjustFunctionTest {
  private final List<Tuple2<Long, Long>> res = new ArrayList<>();
  private final Collector<Tuple2<Long, Long>> out = new ListCollector<>(res);
  private final EventTimeAdjustFunction<Long> f = new EventTimeAdjustFunction<>(Instant::ofEpochMilli);
  private final KeyedOneInputStreamOperatorTestHarness<Long, Long, Tuple2<Long, Long>> testHarness =
    new KeyedOneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(f), x -> x, Types.LONG);

  public EventTimeAdjustFunctionTest() throws Exception {
  }

  @BeforeEach
  public void setupTest() throws Exception {
    res.clear();
    testHarness.open();
  }

  @Test
  public void testNotAdjustTime() {
    Long eventTime = System.currentTimeMillis() - Duration.ofDays(1).toMillis();
    f.flatMap(eventTime, out);
    Assertions.assertEquals(res.get(0).f0, eventTime);
    Assertions.assertEquals(res.get(0).f1, eventTime);
  }

  @Test
  public void testAdjustTime() {
    Long eventTime = System.currentTimeMillis() - 20;
    f.flatMap(eventTime, out);
    Assertions.assertTrue(res.get(0).f0 > eventTime);
    Assertions.assertEquals(res.get(0).f1, eventTime);
  }
}
