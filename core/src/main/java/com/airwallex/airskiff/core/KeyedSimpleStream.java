package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;

import java.util.Collections;
import java.util.List;

public class KeyedSimpleStream<K, T> implements KStream<K, T> {
  public final Stream<T> stream;
  public final NamedSerializableLambda<T, K> toKey;
  public final Class<K> kc;

  public KeyedSimpleStream(Stream<T> stream, NamedSerializableLambda<T, K> toKey, Class<K> kc) {
    this.stream = stream;
    this.toKey = toKey;
    this.kc = kc;
  }

  @Override
  public Class<K> keyClass() {
    return kc;
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
