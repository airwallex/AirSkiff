package com.airwallex.airskiff.core;

import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.config.Config;

import java.util.ArrayList;
import java.util.List;

public class SourceStream<T> implements Stream<T> {
  public Config<T> getConfig() {
    return config;
  }

  public void setConfig(Config<T> config) {
    this.config = config;
  }

  public Config<T> config;

  public SourceStream(Config<T> config) {
    this.config = config;
  }

  @Override
  public Class<T> getClazz() {
    return config.clz();
  }

  @Override
  public List<Stream> parentStreams() {
    return new ArrayList<>();
  }
}
