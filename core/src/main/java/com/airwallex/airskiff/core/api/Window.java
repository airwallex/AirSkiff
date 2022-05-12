package com.airwallex.airskiff.core.api;

import java.io.Serializable;
import java.time.Duration;

/**
 * We will only allow event time based window, since we want to support batch and real time.
 * Processing time makes no sense in batch mode.
 */
public interface Window extends Serializable {
  Duration size();
}
