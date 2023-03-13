package com.airwallex.airskiff.spark;


import com.airwallex.airskiff.common.functions.NamedMonoid;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class CustomKeyedAccumulator<K, T> extends AccumulatorV2<Tuple2<K, T>, Tuple2<K, T>> {

  private Map<K, T> state = new HashMap<>();
  private NamedMonoid<T> monoid;

  public Map<K, T> getState() {
    return state;
  }

  public void setState(Map<K, T> state) {
    this.state = state;
  }

  public NamedMonoid<T> getMonoid() {
    return monoid;
  }

  public void setMonoid(NamedMonoid<T> monoid) {
    this.monoid = monoid;
  }

  public CustomKeyedAccumulator() {

  }

  public CustomKeyedAccumulator(NamedMonoid<T> monoid, Map<K, T> state) {
    this.state = state;
    this.monoid = monoid;
  }

  public CustomKeyedAccumulator(NamedMonoid<T> monoid) {
    this.monoid = monoid;
  }

  @Override
  public boolean isZero() {
    return state.isEmpty();
  }

  @Override
  public AccumulatorV2<Tuple2<K, T>, Tuple2<K, T>> copy() {
    Map<K, T> newState = new HashMap<>();
    newState.putAll(state);
    return new CustomKeyedAccumulator<>(monoid, newState);
  }

  @Override
  public void reset() {
    state = new HashMap<>();
  }

  @Override
  public void add(Tuple2<K, T> v) {
    K key = v._1;
    T value = v._2;
    if (state.containsKey(key)) {
      state.put(key, monoid.plus(state.get(key), value));
    } else {
      state.put(key, value);
    }
  }

  @Override
  public void merge(AccumulatorV2<Tuple2<K, T>, Tuple2<K, T>> other) {
    Map<K, T> otherState = ((CustomKeyedAccumulator<K, T>) other).getState();
    for (Map.Entry<K, T> entry : otherState.entrySet()) {
      K key = entry.getKey();
      T value = entry.getValue();
      if (state.containsKey(key)) {
        state.put(key, monoid.plus(state.get(key), value));
      } else {
        state.put(key, value);
      }
    }
  }

  @Override
  public Tuple2<K, T> value() {
    return Tuple2.apply(null, null);
  }
}
