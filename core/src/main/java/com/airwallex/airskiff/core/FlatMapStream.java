package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.core.api.Stream;

import java.util.Collections;
import java.util.List;

public class FlatMapStream<T, U> implements Stream<U> {
  public final Stream<T> stream;
  public final NamedSerializableLambda<T, Iterable<U>> f;
  public final Class<U> uc;

  public FlatMapStream(Stream<T> stream, NamedSerializableLambda<T, Iterable<U>> f, Class<U> uc) {
    this.stream = stream;
    this.f = f;
    this.uc = uc;
  }

  @Override
  public Class<U> getClazz() {
    return uc;
  }

  @Override
  public List<Stream> parentStreams() {
    return Collections.singletonList(stream);
  }
}
