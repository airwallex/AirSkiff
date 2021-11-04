package com.airwallex.airskiff.flink.types;

import com.airwallex.airskiff.common.Pair;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class JavaPairSerializerSnapshot<L, R>
  extends CompositeTypeSerializerSnapshot<Pair<L, R>, PairSerializer<L, R>> {

  private static final int CURRENT_VERSION = 1;

  public JavaPairSerializerSnapshot() {
    super(PairSerializer.class);
  }

  public JavaPairSerializerSnapshot(PairSerializer<L, R> lrPairSerializer) {
    super(lrPairSerializer);
  }

  @Override
  protected int getCurrentOuterSnapshotVersion() {
    return CURRENT_VERSION;
  }

  @Override
  protected TypeSerializer<?>[] getNestedSerializers(PairSerializer<L, R> lrPairSerializer) {
    return new TypeSerializer<?>[]{
      lrPairSerializer.getLeftSerializer(), lrPairSerializer.getRightSerializer()
    };
  }

  @Override
  protected PairSerializer<L, R> createOuterSerializerWithNestedSerializers(
    TypeSerializer<?>[] typeSerializers
  ) {
    @SuppressWarnings("unchecked") TypeSerializer<L> leftSerializer = (TypeSerializer<L>) typeSerializers[0];

    @SuppressWarnings("unchecked") TypeSerializer<R> rightSerializer = (TypeSerializer<R>) typeSerializers[1];

    return new PairSerializer<>(leftSerializer, rightSerializer);
  }
}
