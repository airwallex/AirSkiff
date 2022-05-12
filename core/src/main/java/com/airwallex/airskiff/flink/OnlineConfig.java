package com.airwallex.airskiff.flink;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.Serializable;
import java.util.Map;

public interface OnlineConfig<T extends SpecificRecordBase> extends Serializable {
  void updateProperties(Map<String, String> props);
}
