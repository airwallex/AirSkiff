package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedSerializableIterableLambda;
import com.airwallex.airskiff.common.functions.SerializableComparator;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.api.Window;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class WindowedStream<K, T, U, W extends Window> implements KStream<K, U> {
  public final KStream<K, T> stream;
  public final W window;
  public final NamedSerializableIterableLambda<T, U> f;
  public final Comparator<T> comparator;
  public final Class<U> uc;

  public WindowedStream(
    KStream<K, T> stream, W w, NamedSerializableIterableLambda<T, U> p, SerializableComparator<T> order, Class<U> uc
  ) {
    this.stream = stream;
    window = w;
    f = p;
    comparator = order;
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
  public List<Stream> parentStreams() {
    return Collections.singletonList(stream);
  }
}
