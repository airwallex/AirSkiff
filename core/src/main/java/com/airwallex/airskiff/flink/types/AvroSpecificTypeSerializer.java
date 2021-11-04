package com.airwallex.airskiff.flink.types;

import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;

public class AvroSpecificTypeSerializer<T> extends TypeSerializer<T> {

  private final AvroSerializer<T> serializer;

  public AvroSpecificTypeSerializer(AvroSerializer<T> s) {
    serializer = s;
    // initialize serializer
    createInstance();
    try {
      // This is a work around for a bug in Avro, which would ignore logicalType if it
      // is in a union, such as
      // {"name":"created_at","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}
      Field privateAvroDataField = AvroSerializer.class.getDeclaredField("avroData");
      privateAvroDataField.setAccessible(true);
      GenericData avroData = (GenericData) privateAvroDataField.get(serializer);
      // we have to hard-code these conversions, because they are not serializable
      // we can't pass them in from flink app unless we wrap them in a serializable object
      avroData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isImmutableType() {
    return serializer.isImmutableType();
  }

  @Override
  public TypeSerializer<T> duplicate() {
    return serializer.duplicate();
  }

  @Override
  public T createInstance() {
    return serializer.createInstance();
  }

  @Override
  public T copy(T t) {
    return serializer.copy(t);
  }

  @Override
  public T copy(T t, T t1) {
    return serializer.copy(t, t1);
  }

  @Override
  public int getLength() {
    return serializer.getLength();
  }

  @Override
  public void serialize(T t, DataOutputView dataOutputView) throws IOException {
    serializer.serialize(t, dataOutputView);
  }

  @Override
  public T deserialize(DataInputView dataInputView) throws IOException {
    return serializer.deserialize(dataInputView);
  }

  @Override
  public T deserialize(T t, DataInputView dataInputView) throws IOException {
    return serializer.deserialize(t, dataInputView);
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
    serializer.copy(dataInputView, dataOutputView);
  }

  public AvroSerializer<T> underlying() {
    return serializer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof AvroSpecificTypeSerializer) {
      return serializer.equals(((AvroSpecificTypeSerializer) o).underlying());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return serializer.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    return serializer.snapshotConfiguration();
  }
}
