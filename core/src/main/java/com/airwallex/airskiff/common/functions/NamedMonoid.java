package com.airwallex.airskiff.common.functions;

import java.io.Serializable;

public interface NamedMonoid<T> extends Serializable {
  default String name() {
    return "";
  }

  default T zero() {
    return null;
  }

  // should handle cases where t1 is null
  T plus(T t1, T t2);
}
