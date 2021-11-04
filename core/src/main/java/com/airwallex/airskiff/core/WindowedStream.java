package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.functions.NamedSerializableIterableLambda;
import com.airwallex.airskiff.common.functions.SerializableComparator;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.api.Window;
import java.util.Comparator;

public class WindowedStream<K, T, U, W extends Window> implements Stream<U> {
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
  public Class<U> getClazz() {
    return uc;
  }
}
