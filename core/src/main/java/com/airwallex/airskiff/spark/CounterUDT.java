package com.airwallex.airskiff.spark;


import com.airwallex.airskiff.examples.Counter;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import java.io.*;

public class CounterUDT extends UserDefinedType<Counter> {
  @Override
  public DataType sqlType() {
    return Encoders.bean(Counter.class).schema();
  }

  @Override
  public Object serialize(Counter obj) {
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
  public Counter deserialize(Object datum) {
    try {
      var bis = new ByteArrayInputStream((byte[]) datum);
      var ois = new ObjectInputStream(bis);
//      return new Counter(ois.readUTF(), ois.readLong());
      return (Counter) ois.readObject();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<Counter> userClass() {
    return Counter.class;
  }
}
