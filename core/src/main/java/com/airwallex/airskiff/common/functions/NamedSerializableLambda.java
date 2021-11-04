package com.airwallex.airskiff.common.functions;

public interface NamedSerializableLambda<I, O> extends SerializableLambda<I, O> {
  default String name() {
    // TODO: use reflection to populate this
    return "";
  }
}
