package com.airwallex.airskiff.core.config;

import java.io.Serializable;

/**
 * Represents a config for Stream's data sources.
 */
public interface Config<T> extends Serializable {
  Class<T> clz();
}
