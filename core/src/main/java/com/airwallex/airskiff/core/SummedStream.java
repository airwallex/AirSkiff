package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedMonoid;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;

import java.util.Collections;
import java.util.List;

public class SummedStream<K, T> implements KStream<K, T> {
  public final KStream<K, T> stream;
  public final NamedMonoid<T> monoid;

  public SummedStream(KStream<K, T> stream, NamedMonoid<T> monoid) {
    this.stream = stream;
    this.monoid = monoid;
  }

  @Override
  public Class<K> keyClass() {
    return stream.keyClass();
  }

  @Override
  public Class<Pair<K, T>> getClazz() {
    return stream.getClazz();
  }

  @Override
  public List<Stream> parentStreams() {
    return Collections.singletonList(stream);
  }
}
