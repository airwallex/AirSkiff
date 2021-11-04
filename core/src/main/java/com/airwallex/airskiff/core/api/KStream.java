package com.airwallex.airskiff.core.api;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedSerializableIterableLambda;
import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.common.functions.NamedMonoid;

import com.airwallex.airskiff.common.functions.SerializableComparator;
import com.airwallex.airskiff.core.LeftJoinStream;
import com.airwallex.airskiff.core.MapValueStream;
import com.airwallex.airskiff.core.OrderedSummedStream;

import com.airwallex.airskiff.core.PreviousMonoid;
import com.airwallex.airskiff.core.StreamUtils;
import com.airwallex.airskiff.core.SummedStream;
import com.airwallex.airskiff.core.WindowedStream;

public interface KStream<K, T> extends Stream<Pair<K, T>> {
  /**
   * Windowing on a keyed stream. The window behavior depends on the window type passed in
   *
   * @param w     a window definition
   * @param p     the lambda would be passed in a iterator of T, which contains all T's received in the
   *              said window W
   * @param order T must be comparable, so we can have total determinism
   * @param uc    Class of the output type
   */
  default <U, W extends Window> WindowedStream<K, T, U, W> window(
    W w, NamedSerializableIterableLambda<T, U> p, SerializableComparator<T> order, Class<U> uc
  ) {
    return new WindowedStream<>(this, w, p, order, uc);
  }

  /**
   * Sum the keyed stream using a monoid
   */
  default SummedStream<K, T> sum(NamedMonoid<T> monoid) {
    return new SummedStream<>(this, monoid);
  }

  /**
   * Returns a Pair of (previousValue, currentValue)
   */
  default KStream<K, Pair> previousAndCurrent() {
    return mapValue(t -> new Pair<>(t, t), Pair.class).sum(new PreviousMonoid());
  }

  /**
   * This method is helpful if we want to make the order of events more deterministic.
   * But it may only makes sense in a windowed context or a batch context, as we
   * need multiple events to be compared with each other at the same time.
   */
  default OrderedSummedStream<K, T> orderedSum(
    NamedMonoid<T> monoid, SerializableComparator<T> order
  ) {
    return new OrderedSummedStream<>(this, monoid, order);
  }

  /**
   * This method is similar to leftJoin in SQL, where all T's in the current stream will be in the
   * output Pair(T,U). If a key for a T does not exist in the `other` stream to be joined, U will be
   * null in the output pair.
   */
  default <U> LeftJoinStream<K, T, U> leftJoin(KStream<K, U> other) {
    return new LeftJoinStream<>(this, other);
  }

  Class<K> keyClass();

  default Stream<T> values() {
    return this.map(p -> p.r, StreamUtils.kStreamClass(this));
  }

  /**
   * Only map the value of the KStream, and still keep the same key
   */
  default <U> KStream<K, U> mapValue(NamedSerializableLambda<T, U> fn, Class<U> uc) {
    return new MapValueStream<>(this, fn, uc);
  }
}
