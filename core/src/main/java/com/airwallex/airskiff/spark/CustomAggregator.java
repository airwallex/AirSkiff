package com.airwallex.airskiff.spark;

import com.airwallex.airskiff.common.functions.NamedSerializableIterableLambda;
import com.google.api.client.util.Lists;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class CustomAggregator<T, U> extends Aggregator<T, SerializableList<T>, U> {
  public CustomAggregator(){

  }

  public CustomAggregator(NamedSerializableIterableLambda<T, U> lambda, Class<U> clz) {
    this.lambda = lambda;
    this.clz = clz;
  }

  private NamedSerializableIterableLambda<T, U> lambda;
  private Class<U> clz;

  @Override
  public SerializableList<T> zero() {
    return new SerializableList<>(new ArrayList<>());
  }

  @Override
  public SerializableList<T> reduce(SerializableList<T> b, T a) {
    if (a != null) {
      b.getList().add(a);
    }
    return b;
  }

  @Override
  public SerializableList<T> merge(SerializableList<T> s1, SerializableList<T> s2) {
    if (s1.getList().isEmpty()) {
      return s2;
    }
    if (s2.getList().isEmpty()) {
      return s1;
    }
    ArrayList<T> b1 = s1.getList();
    ArrayList<T> b2 = s2.getList();
    ArrayList<T> result = (ArrayList<T>) Stream.concat(b1.stream(), b2.stream()).collect(Collectors.toList());
    return new SerializableList<>(result);
  }

  @Override
  public U finish(SerializableList<T> reduction) {
    if (reduction.getList().isEmpty()) {
      return null;
    }
    ArrayList<U> us = Lists.newArrayList(lambda.apply(reduction.getList()));
    return us.get(us.size() - 1);
  }

  @Override
  public Encoder<SerializableList<T>> bufferEncoder() {
    Class<SerializableList<T>> c = (Class<SerializableList<T>>) new SerializableList<T>(new ArrayList<>()).getClass();
    return Utils.encode(c);
  }

  @Override
  public Encoder<U> outputEncoder() {
    return Encoders.bean(clz);
  }
}
