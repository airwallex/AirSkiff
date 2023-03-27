package com.airwallex.airskiff.spark;

import java.io.Serializable;

public class KeyedItem<K, T, U> implements Serializable {
  public KeyedItem() {

  }

  private Long ts;
  private K key;
  private T val1;

  public KeyedItem(Long ts, K key, T val1, U val2) {
    this.ts = ts;
    this.key = key;
    this.val1 = val1;
    this.val2 = val2;
  }

  private U val2;


  public Long getTs() {
    return ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public T getVal1() {
    return val1;
  }

  public void setVal1(T val1) {
    this.val1 = val1;
  }

  public U getVal2() {
    return val2;
  }

  public void setVal2(U val2) {
    this.val2 = val2;
  }
}
