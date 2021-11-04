package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.core.api.Stream;

public class MapStream<T, U> implements Stream<U> {
  public final Stream<T> stream;
  public final NamedSerializableLambda<T, U> f;
  public final Class<U> uc;

  public MapStream(Stream<T> stream, NamedSerializableLambda<T, U> f, Class<U> uc) {
    this.stream = stream;
    this.f = f;
    this.uc = uc;
  }

  @Override
  public Class<U> getClazz() {
    return uc;
  }
}
