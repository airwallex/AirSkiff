package com.airwallex.airskiff.core;

import com.airwallex.airskiff.core.api.Window;
import java.time.Duration;

public class EventTimeBasedSlidingWindow implements Window {
  private final Duration size;
  private final Duration slide;

  public EventTimeBasedSlidingWindow(Duration size, Duration slide) {
    this.size = size;
    this.slide = slide;
  }

  @Override
  public Duration size() {
    return size;
  }

  public Duration slide() {
    return slide;
  }
}
