package com.airwallex.airskiff.core;

import com.airwallex.airskiff.core.api.Stream;

import java.util.ArrayList;
import java.util.List;

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

  @Override
  public List<Stream> upstreams() {
    List<Stream> ups = new ArrayList<>();
    ups.add(a);
    ups.add(b);
    return ups;
  }
}
