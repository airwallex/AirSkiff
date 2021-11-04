package com.airwallex.airskiff.flink;

import java.io.Serializable;
import java.util.Map;
import org.apache.avro.specific.SpecificRecordBase;

public interface OnlineConfig<T extends SpecificRecordBase> extends Serializable {
  void updateProperties(Map<String, String> props);
}
