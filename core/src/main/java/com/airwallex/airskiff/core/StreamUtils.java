package com.airwallex.airskiff.core;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.api.Window;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;

public class StreamUtils implements Serializable {
  public static <T> Class<T> clz(Stream<T> stream) {
    if (stream instanceof SourceStream) {
      return ((SourceStream<T>) stream).config.clz();
    }
    if (stream instanceof FlatMapStream) {
      return ((FlatMapStream<?, T>) stream).uc;
    }
    if (stream instanceof MapStream) {
      return ((MapStream<?, T>) stream).uc;
    }
    if (stream instanceof WindowedStream) {
      return ((WindowedStream<?, ?, T, ? extends Window>) stream).uc;
    }
    if (stream instanceof KStream) {
      return kStreamClass((KStream<?, T>) stream);
    }
    if (stream instanceof ConcatStream) {
      return clz(((ConcatStream<T>) stream).a);
    }
    if (stream instanceof SqlStream) {
      return ((SqlStream<?, T>) stream).tc;
    }
    if (stream instanceof FilterStream) {
      return clz(((FilterStream<T>) stream).stream);
    }
    throw new IllegalArgumentException("Unknown stream type");
  }

  public static <K, T> Class<T> kStreamClass(KStream<K, T> stream) {
    if (stream instanceof KeyedSimpleStream) {
      return ((KeyedSimpleStream<K, T>) stream).stream.getClazz();
    }
    if (stream instanceof SummedStream) {
      return kStreamClass(((SummedStream<K, T>) stream).stream);
    }
    if (stream instanceof OrderedSummedStream) {
      return kStreamClass(((OrderedSummedStream<K, T>) stream).stream);
    }
    if (stream instanceof LeftJoinStream) {
      return (Class<T>) Pair.class;
    }
    if (stream instanceof MapValueStream) {
      return ((MapValueStream<?, ?, T>) stream).uc;
    }
    throw new IllegalArgumentException("Unknown stream type");
  }

  public static <T> Field[] getFields(Class<T> tc) {
    return Arrays.stream(tc.getDeclaredFields())
      .filter(f -> !Modifier.isStatic(f.getModifiers()))
      .toArray(Field[]::new);
  }
}
