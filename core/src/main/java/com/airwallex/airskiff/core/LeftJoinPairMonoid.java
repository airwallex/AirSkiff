package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedMonoid;

public class LeftJoinPairMonoid<T, U> implements NamedMonoid<Pair<T, U>> {
  @Override
  public Pair<T, U> plus(Pair<T, U> t1, Pair<T, U> t2) {
    if (t1 == null) {
      return t2;
    }
    U u = t2.r == null ? t1.r : t2.r;
    return new Pair<>(t2.l, u);
  }
}
