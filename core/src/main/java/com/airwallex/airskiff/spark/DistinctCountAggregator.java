package com.airwallex.airskiff.spark;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.HashSet;

public class DistinctCountAggregator<T> extends Aggregator<T, HashSet<T>, Integer> {

  public DistinctCountAggregator() {
  }

  @Override
  public HashSet<T> zero() {
    return new HashSet<>();
  }

  @Override
  public HashSet<T> reduce(HashSet<T> buffer, T input) {
    if (input != null) {
      buffer.add(input);
    }
    return buffer;
  }

  @Override
  public HashSet<T> merge(HashSet<T> buffer1, HashSet<T> buffer2) {
    if (buffer1 == null) {
      return buffer2;
    }
    if (buffer2 == null) {
      return buffer1;
    }
    if (buffer1 == null && buffer2 == null) {
      return new HashSet<>();
    }
    buffer1.addAll(buffer2);
    return buffer1;
  }

  @Override
  public Integer finish(HashSet<T> reduction) {
    return reduction.size();
  }

  @Override
  public Encoder<HashSet<T>> bufferEncoder() {
    var clz = (Class<HashSet<T>>) new HashSet<T>().getClass();
    return Encoders.javaSerialization(clz);
  }

  @Override
  public Encoder<Integer> outputEncoder() {
    return Encoders.INT();
  }
}

