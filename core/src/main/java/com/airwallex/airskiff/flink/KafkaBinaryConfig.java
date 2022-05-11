package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

public class KafkaBinaryConfig implements Serializable {
  public static final TypeInformation<Pair<Map<String, byte[]>, byte[]>> type =
    new PairTypeInfo<>(new MapTypeInfo<>(STRING_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      BYTE_PRIMITIVE_ARRAY_TYPE_INFO
    );
  private final Properties properties;

  public KafkaBinaryConfig() {
    this(new Properties());
  }

  public KafkaBinaryConfig(Properties properties) {
    this.properties = properties;
  }

  public void updateProperties(Map<String, String> props) {
    properties.putAll(props);
  }

  // We use this producer to push records to the target topic based on the source topic name
  // We extract the source topic name from the record header `topic`
  // This is why we need a param `prefix` to name the target topic
  // Warning: We must avoid matching the source topic subscription pattern
  // For example: if you subscribe m123_*, and you set the target topic as m123_versioned :(
  public FlinkKafkaProducer<Pair<Map<String, byte[]>, byte[]>> toProducer(String topicPrefix) {
    return new FlinkKafkaProducer<>(topicPrefix + "_default", // We must set topic for every record
      (p, timestamp) -> {
        var topic = new String(p.l.get("topic"), StandardCharsets.UTF_8);
        if (topic.contains("ml_platform")) {
          var newTopic = topic.replace("ml_platform", "ml_platform_versioned");
          var record = new ProducerRecord<byte[], byte[]>(newTopic, null, p.r);
          p.l.forEach((key, value) -> record.headers().add(key, value));
          return record;
        } else {
          throw new IllegalArgumentException(topic + " is not valid. Must contain 'ml_platform'.");
        }
      }, properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    );
  }

  public FlinkKafkaConsumer<Pair<Map<String, byte[]>, byte[]>> toConsumer(Pattern topicPattern) {
    return new FlinkKafkaConsumer<>(topicPattern, new KafkaDeserializationSchema<>() {
      @Override
      public TypeInformation<Pair<Map<String, byte[]>, byte[]>> getProducedType() {
        return type;
      }

      @Override
      public boolean isEndOfStream(Pair<Map<String, byte[]>, byte[]> nextElement) {
        return false;
      }

      @Override
      public Pair<Map<String, byte[]>, byte[]> deserialize(
        ConsumerRecord<byte[], byte[]> record
      ) {
        final byte[] value = record.value();
        final Map<String, byte[]> headers = new HashMap<>();

        headers.put("topic", record.topic().getBytes(StandardCharsets.UTF_8));
        record.headers().forEach((Header h) -> {
          String k = h.key();
          byte[] v = h.value();
          headers.put(k, v);
        });
        return new Pair<>(headers, value);
      }
    }, properties);
  }
}
