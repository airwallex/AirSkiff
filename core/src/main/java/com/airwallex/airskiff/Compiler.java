package com.airwallex.airskiff;

import com.airwallex.airskiff.core.api.Stream;

/**
 * Compiles a Stream into a data representation that is supported
 * by an underlying big data framework, such as Flink, Spark, etc.
 */
public interface Compiler<P> {
  <T> P compile(Stream<T> stream);
}
