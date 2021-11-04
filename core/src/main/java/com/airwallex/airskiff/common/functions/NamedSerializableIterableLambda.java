package com.airwallex.airskiff.common.functions;

import java.io.Serializable;

public interface NamedSerializableIterableLambda<I, O> extends Serializable {
  Iterable<O> apply(Iterable<I> in);

  default String name() {
    // TODO: use reflection to populate this
    return "";
  }
}
