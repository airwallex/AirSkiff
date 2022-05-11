package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.config.Config;

import java.util.List;

public class ListSourceConfig<T> implements Config<T> {
  private final List<Pair<Long, T>> data;
  private final Class<T> tc;

  public ListSourceConfig(List<Pair<Long, T>> data, Class<T> tc) {
    this.data = data;
    this.tc = tc;
  }

  @Override
  public Class<T> clz() {
    return tc;
  }

  public List<Pair<Long, T>> source() {
    return data;
  }
}
