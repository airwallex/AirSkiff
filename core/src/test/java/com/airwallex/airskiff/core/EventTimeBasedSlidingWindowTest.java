package com.airwallex.airskiff.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

public class EventTimeBasedSlidingWindowTest {
  @Test
  public void testWindow() {
    Duration size = Duration.ofSeconds(10);
    Duration slide = Duration.ofSeconds(1);
    EventTimeBasedSlidingWindow w = new EventTimeBasedSlidingWindow(size, slide);

    Assertions.assertEquals(w.size(), size);
    Assertions.assertEquals(w.slide(), slide);
  }
}
