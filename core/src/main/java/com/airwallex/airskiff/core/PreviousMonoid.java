package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedMonoid;

public class PreviousMonoid implements NamedMonoid<Pair> {
  @Override
  public Pair plus(Pair t1, Pair t2) {
    // for t2, l == r
    if (t1 == null) {
      return new Pair<>(null, t2.r);
    }
    return new Pair<>(t1.r, t2.r);
  }
}
