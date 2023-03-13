package com.airwallex.airskiff.spark;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class Utils {
  public static <K> Encoder<K> encode(Class<K> clazz) {
    if (clazz == String.class) {
      return (Encoder<K>) Encoders.STRING();
    }
    if (clazz == Integer.class) {
      return (Encoder<K>) Encoders.INT();
    }
    if (clazz == Long.class) {
      return (Encoder<K>) Encoders.LONG();
    }
    if (clazz == Double.class) {
      return (Encoder<K>) Encoders.DOUBLE();
    }
    if (clazz == Float.class) {
      return (Encoder<K>) Encoders.FLOAT();
    }
    if (clazz == Boolean.class) {
      return (Encoder<K>) Encoders.BOOLEAN();
    }
    if (clazz == Byte.class) {
      return (Encoder<K>) Encoders.BYTE();
    }
    if (clazz == Short.class) {
      return (Encoder<K>) Encoders.SHORT();
    }
    if (clazz == Character.class) {
      return (Encoder<K>) Encoders.STRING();
    }
    if (clazz == byte[].class) {
      return (Encoder<K>) Encoders.BINARY();
    }
    return Encoders.bean(clazz);
  }
}
