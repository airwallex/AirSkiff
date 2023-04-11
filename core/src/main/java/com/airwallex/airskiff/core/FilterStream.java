package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.core.api.Stream;

import java.util.Collections;
import java.util.List;

public class FilterStream<T> implements Stream<T> {
  public final Class<T> tc;
  public final Stream<T> stream;
  public final NamedSerializableLambda<T, Boolean> p;

  public FilterStream(Stream<T> stream, NamedSerializableLambda<T, Boolean> p) {
    tc = StreamUtils.clz(stream);
    this.stream = stream;
    this.p = p;
  }

  @Override
  public Class<T> getClazz() {
    return tc;
  }

  @Override
  public List<Stream> parentStreams() {
    return Collections.singletonList(stream);
  }
}
