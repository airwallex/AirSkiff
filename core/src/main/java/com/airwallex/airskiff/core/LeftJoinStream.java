package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.api.KStream;

public class LeftJoinStream<K, T, U> implements KStream<K, Pair<T, U>> {
  public final KStream<K, T> s1;
  public final KStream<K, U> s2;

  public LeftJoinStream(KStream<K, T> s1, KStream<K, U> s2) {
    this.s1 = s1;
    this.s2 = s2;
  }

  @Override
  public Class<K> keyClass() {
    return s1.keyClass();
  }

  @Override
  public Class getClazz() {
    return Pair.class;
  }
}
