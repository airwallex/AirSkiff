package com.airwallex.airskiff.spark;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class Utils {
  private static <K> Encoder<K> encodeBasics(Class<K> clazz) {
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
    return null;
  }

  public static <K> Encoder<K> encode(Class<K> clazz) {
    Encoder<K> result = encodeBasics(clazz);
    if (result != null) {
      return result;
    }

    return Encoders.bean(clazz);
  }

  public static <K> Encoder<K> encodeAvro(Class<K> clazz) {
    Encoder<K> kEncoder = encodeBasics(clazz);
    if (kEncoder != null) {
      return kEncoder;
    }
    return Encoders.javaSerialization(clazz);
  }

  public static <K> Encoder<K> encodeJava(Class<K> clazz) {
    Encoder<K> kEncoder = encodeBasics(clazz);
    if (kEncoder != null) {
      return kEncoder;
    }
    return Encoders.javaSerialization(clazz);
  }

  public static <K> Encoder<K> encodeBean(Class<K> clazz) {
    Encoder<K> kEncoder = encodeBasics(clazz);
    if (kEncoder != null) {
      return kEncoder;
    }
    return Encoders.bean(clazz);
  }

}
