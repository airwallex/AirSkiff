package com.airwallex.airskiff.flink.types;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;

public class AvroSpecificTypeInfo<T extends SpecificRecordBase> extends AvroTypeInfo<T> {
  public AvroSpecificTypeInfo(Class<T> typeClass) {
    super(typeClass);
  }

  @Override
  public TypeSerializer<T> createSerializer(ExecutionConfig config) {
    return new AvroSpecificTypeSerializer<>(new AvroSerializer<>(this.getTypeClass()));
  }
}
