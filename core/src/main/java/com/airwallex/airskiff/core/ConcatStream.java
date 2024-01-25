package com.airwallex.airskiff.core;

import com.airwallex.airskiff.core.api.Stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ConcatStream<T> implements Stream<T> {
  public final Stream<T> a;
  public final Stream<T> b;
  public final Duration allowedLatency;

  public ConcatStream(Stream<T> a, Stream<T> b) {
    this.a = a;
    this.b = b;
    this.allowedLatency = Duration.ZERO;
  }

  public ConcatStream(Stream<T> a, Stream<T> b, Duration allowedLatency) {
    this.a = a;
    this.b = b;
    this.allowedLatency = allowedLatency;
  }

  @Override
  public Class<T> getClazz() {
    return a.getClazz();
  }

  @Override
  public List<Stream> parentStreams() {
    List<Stream> ups = new ArrayList<>();
    ups.add(a);
    ups.add(b);
    return ups;
  }
}
