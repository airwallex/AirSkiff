package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;

import java.util.Collections;
import java.util.List;

public class MapValueStream<K, T, U> implements KStream<K, U> {
  public final KStream<K, T> stream;
  public final NamedSerializableLambda<T, U> fn;
  public final Class<U> uc;

  public MapValueStream(KStream<K, T> stream, NamedSerializableLambda<T, U> fn, Class<U> uc) {
    this.stream = stream;
    this.fn = fn;
    this.uc = uc;
  }

  @Override
  public Class<K> keyClass() {
    return stream.keyClass();
  }

  @Override
  public Class getClazz() {
    return Pair.class;
  }

  @Override
  public List<Stream> upstreams() {
    return Collections.singletonList(stream);
  }
}
