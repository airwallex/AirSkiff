package com.airwallex.airskiff.flink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class EventTimeManagerTest {

  @Test
  void testUsingEventTimeBeforeCaughtUp() {
    EventTimeManager eventTimeManager = new EventTimeManager();
    long currentTime = System.currentTimeMillis();
    long eventTime = currentTime - Constants.TEN_SECONDS.toMillis() - 1000;
    long adjustTime = eventTimeManager.adjust(eventTime, currentTime);
    Assertions.assertEquals(eventTime, adjustTime);
    Assertions.assertFalse(eventTimeManager.isCaughtUp());
  }

  @Test
  void testUsingProcessingTimeWhenCaughtUp() {
    EventTimeManager eventTimeManager = new EventTimeManager();
    long currentTime = System.currentTimeMillis();
    long eventTime = currentTime - Constants.TEN_SECONDS.toMillis() + 1000;
    long adjustTime = eventTimeManager.adjust(eventTime, currentTime);
    Assertions.assertEquals(adjustTime, currentTime);
    Assertions.assertTrue(eventTimeManager.isCaughtUp());
  }

  @Test
  void testUsingProcessingTimeAfterCaughtUp() {
    EventTimeManager eventTimeManager = new EventTimeManager();
    eventTimeManager.caughtUp = true;
    long currentTime = System.currentTimeMillis();
    long eventTime = currentTime - Constants.TEN_SECONDS.toMillis() - 1000;
    long adjustTime = eventTimeManager.adjust(eventTime, currentTime);
    Assertions.assertEquals(adjustTime, currentTime);
  }

  @Test
  void testMonotonicTime() {
    EventTimeManager eventTimeManager = new EventTimeManager();
    eventTimeManager.caughtUp = true;
    List<Long> timeList = List.of(1L, 1L, 2L, 3L, 7L, 5L);
    List<Long> result = new ArrayList<>();
    for (Long ts : timeList) {
      long adjust = eventTimeManager.adjust(0, ts);
      result.add(adjust);
    }
    Assertions.assertArrayEquals(List.of(1L, 2L, 3L, 4L, 7L, 8L).toArray(), result.toArray());
  }
}
