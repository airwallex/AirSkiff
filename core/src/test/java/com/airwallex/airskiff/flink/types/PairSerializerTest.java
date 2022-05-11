package com.airwallex.airskiff.flink.types;

import com.airwallex.airskiff.common.Pair;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class PairSerializerTest {
  @Test
  public void testPairSerializerCopy() throws IOException {
    PairSerializer<Pair<String, Pair<String, Integer>>, Long> serializer =
      new PairSerializer<>(new PairSerializer<>(new StringSerializer(),
        new PairSerializer<>(new StringSerializer(), new IntSerializer())
      ),
        new LongSerializer()
      );

    DataOutputSerializer do1 = new DataOutputSerializer(10);
    serializer.serialize(new Pair<>(new Pair<>("a", new Pair<>("b", 1)), 2L), do1);

    DataInputViewStreamWrapper di = new DataInputViewStreamWrapper(new ByteArrayInputStream(do1.getCopyOfBuffer()));

    DataOutputSerializer do2 = new DataOutputSerializer(10);
    serializer.copy(di, do2);

    Assertions.assertEquals(do1.length(), do2.length());
  }

  @Test
  public void testPairSerializerCopyWithNullValue() throws IOException {
    PairSerializer<Pair<String, Pair<Double, Integer>>, Long> serializer =
      new PairSerializer<>(new PairSerializer<>(new StringSerializer(),
        new PairSerializer<>(new DoubleSerializer(), new IntSerializer())
      ),
        new LongSerializer()
      );

    DataOutputSerializer do1 = new DataOutputSerializer(10);
    serializer.serialize(new Pair<>(new Pair<>("a", new Pair<>(null, 1)), null), do1);
    DataInputViewStreamWrapper di = new DataInputViewStreamWrapper(new ByteArrayInputStream(do1.getCopyOfBuffer()));
    DataOutputSerializer do2 = new DataOutputSerializer(10);
    serializer.copy(di, do2);
    Assertions.assertEquals(do1.length(), do2.length());
  }


  @Test
  public void testPairSerializerRoundTrip() throws IOException {
    PairSerializer<Pair<String, Pair<String, Integer>>, Long> serializer =
      new PairSerializer<>(new PairSerializer<>(new StringSerializer(),
        new PairSerializer<>(new StringSerializer(), new IntSerializer())
      ),
        new LongSerializer()
      );

    DataOutputSerializer do1 = new DataOutputSerializer(10);
    Pair<Pair<String, Pair<String, Integer>>, Long> orig = new Pair<>(new Pair<>("a", null), 2L);
    serializer.serialize(orig, do1);

    DataInputViewStreamWrapper di = new DataInputViewStreamWrapper(new ByteArrayInputStream(do1.getCopyOfBuffer()));
    Pair<Pair<String, Pair<String, Integer>>, Long> p = serializer.deserialize(di);

    Assertions.assertEquals(orig, p);
  }
}
