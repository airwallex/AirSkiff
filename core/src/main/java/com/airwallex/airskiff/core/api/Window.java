package com.airwallex.airskiff.core.api;

import java.io.Serializable;
import java.time.Duration;

/**
 * A time based window that can be a sliding window, a tumbling window, or
 * in some other format.
 */
public interface Window extends Serializable {
  Duration size();
}
