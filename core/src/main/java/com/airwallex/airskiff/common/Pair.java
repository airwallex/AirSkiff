package com.airwallex.airskiff.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public class Pair<L, R> implements Serializable, Cloneable {
  @Override
  protected Pair<L, R> clone() throws CloneNotSupportedException {
    return new Pair<>(l, r);
  }

  public L getL() {
    return l;
  }

  public void setL(L l) {
    this.l = l;
  }

  public R getR() {
    return r;
  }

  public void setR(R r) {
    this.r = r;
  }

  public L l;
  public R r;

  public Pair() {
  }

  public Pair(L l, R r) {
    this.l = l;
    this.r = r;
  }

  @Override
  public String toString() {
    return "Pair{" + "l=" + l + ", r=" + r + '}';
  }

  @Override
  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }
}
