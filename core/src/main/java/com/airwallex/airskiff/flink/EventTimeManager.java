package com.airwallex.airskiff.flink;

public class EventTimeManager {

  volatile boolean caughtUp = false;
  long lastProcessTime;

  public long adjust(long eventTime, long currentTime) {
    if (caughtUp) {
      long retValue = Math.max(lastProcessTime + 1, currentTime);
      lastProcessTime = retValue;
      return retValue;
    }

    long delay = currentTime - eventTime;
    if (delay > Constants.TEN_SECONDS.toMillis()) {
      return eventTime;
    }
    // caught up for the first time.
    checkCaughtUp(delay);
    lastProcessTime = currentTime;
    return currentTime;
  }

  public void checkCaughtUp(long delay) {
    if (delay <= Constants.TEN_SECONDS.toMillis()) {
      caughtUp = true;
    }
  }

  public boolean isCaughtUp() {
    return caughtUp;
  }
}
