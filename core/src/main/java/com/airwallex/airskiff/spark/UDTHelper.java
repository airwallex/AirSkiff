package com.airwallex.airskiff.spark;


import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;

import java.io.*;

public class UDTHelper {
  public UDTHelper() {
  }

  public static <T> UserDefinedType<T> genClass(Class<T> clazz) {
    return new UserDefinedType<T>() {
      void init() {
      }
      @Override
      public Class<T> userClass() {
        return clazz;
      }

      @Override
      public T deserialize(Object datum) {
        try {
          var bis = new ByteArrayInputStream((byte[]) datum);
          var ois = new ObjectInputStream(bis);
          return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Object serialize(T obj) {
        try {
          var bos = new ByteArrayOutputStream();
          try (var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
          }
          return bos.toByteArray();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public DataType sqlType() {
        return DataTypes.BinaryType;
      }
    };
  }
}
