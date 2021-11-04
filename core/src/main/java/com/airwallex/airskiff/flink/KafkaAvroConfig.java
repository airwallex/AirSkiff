package com.airwallex.airskiff.flink;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

public class KafkaAvroConfig<T extends SpecificRecordBase> implements OnlineConfig<T> {
  private final Pattern topicPattern;
  private final Properties properties;
  private final KafkaDeserializationSchemaWrapper<T> schema;

  public KafkaAvroConfig(Pattern topicPattern, Class<T> clazz) {
    this(
      topicPattern,
      new Properties(),
      new KafkaDeserializationSchemaWrapper<>(AvroDeserializationSchema.forSpecific(clazz))
    );
  }

  public KafkaAvroConfig(
    Pattern topicPattern, Properties properties, KafkaDeserializationSchemaWrapper<T> schema
  ) {
    this.topicPattern = topicPattern;
    this.properties = properties;
    this.schema = schema;
  }

  @Override
  public void updateProperties(Map<String, String> props) {
    properties.putAll(props);
  }

  public FlinkKafkaConsumer<T> toConsumer() {
    return new FlinkKafkaConsumer<>(topicPattern, schema, properties);
  }
}
