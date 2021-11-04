package com.airwallex.airskiff.core;

import com.airwallex.airskiff.core.api.Stream;

public class ConcatStream<T> implements Stream<T> {
  public final Stream<T> a;
  public final Stream<T> b;

  public ConcatStream(Stream<T> a, Stream<T> b) {
    this.a = a;
    this.b = b;
  }

  @Override
  public Class<T> getClazz() {
    return a.getClazz();
  }
}
