package com.airwallex.airskiff.flink.types;

import com.airwallex.airskiff.common.Pair;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class PairSerializer<L, R> extends TypeSerializer<Pair<L, R>> {
  private static final long serialVersionUID = 1L;

  private final TypeSerializer<L> leftSerializer;
  private final TypeSerializer<R> rightSerializer;

  public PairSerializer(TypeSerializer<L> leftSerializer, TypeSerializer<R> rightSerializer) {
    this.leftSerializer = leftSerializer;
    this.rightSerializer = rightSerializer;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Pair<L, R>> duplicate() {
    TypeSerializer<L> duplicateLeft = leftSerializer.duplicate();
    TypeSerializer<R> duplicateRight = rightSerializer.duplicate();

    if ((leftSerializer != duplicateLeft) || (rightSerializer != duplicateRight)) {
      // stateful
      return new PairSerializer<L, R>(duplicateLeft, duplicateRight);
    } else {
      return this;
    }
  }

  @Override
  public Pair<L, R> createInstance() {
    return new Pair<>(leftSerializer.createInstance(), rightSerializer.createInstance());
  }

  @Override
  public Pair<L, R> copy(Pair<L, R> from) {
    if (from == null) {
      return null;
    }
    return new Pair<>(leftSerializer.copy(from.l), rightSerializer.copy(from.r));
  }

  @Override
  public Pair<L, R> copy(Pair<L, R> from, Pair<L, R> reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Pair<L, R> lrPair, DataOutputView dataOutputView) throws IOException {
    if (lrPair == null) {
      dataOutputView.writeBoolean(true);
    } else {
      dataOutputView.writeBoolean(false);
      if (lrPair.l == null) {
        dataOutputView.writeBoolean(true);
      } else {
        dataOutputView.writeBoolean(false);
        leftSerializer.serialize(lrPair.l, dataOutputView);
      }
      if (lrPair.r == null) {
        dataOutputView.writeBoolean(true);
      } else {
        dataOutputView.writeBoolean(false);
        rightSerializer.serialize(lrPair.r, dataOutputView);
      }
    }
  }

  @Override
  public Pair<L, R> deserialize(DataInputView dataInputView) throws IOException {
    boolean isNull = dataInputView.readBoolean();
    if (isNull) {
      return null;
    } else {
      L l = null;
      R r = null;
      boolean isNullLeft = dataInputView.readBoolean();
      if (!isNullLeft) l = leftSerializer.deserialize(dataInputView);
      boolean isNullRight = dataInputView.readBoolean();
      if (!isNullRight) r = rightSerializer.deserialize(dataInputView);
      return new Pair<>(l, r);
    }
  }

  @Override
  public Pair<L, R> deserialize(Pair<L, R> reuse, DataInputView dataInputView) throws IOException {
    return deserialize(dataInputView);
  }

  @Override
  public void copy(DataInputView from, DataOutputView to) throws IOException {
    boolean isNull = from.readBoolean();
    to.writeBoolean(isNull);
    if (!isNull) {
      boolean isLeftNull = from.readBoolean();
      to.writeBoolean(isLeftNull);
      if (!isLeftNull) leftSerializer.copy(from, to);
      boolean isRightNull = from.readBoolean();
      to.writeBoolean(isRightNull);
      if (!isRightNull) rightSerializer.copy(from, to);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PairSerializer) {
      PairSerializer<L, R> other = (PairSerializer<L, R>) o;

      return leftSerializer.equals(other.leftSerializer) && rightSerializer.equals(other.rightSerializer);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 17 * leftSerializer.hashCode() + rightSerializer.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Pair<L, R>> snapshotConfiguration() {
    return new JavaPairSerializerSnapshot<>(this);
  }

  public TypeSerializer<L> getLeftSerializer() {
    return leftSerializer;
  }

  public TypeSerializer<R> getRightSerializer() {
    return rightSerializer;
  }
}
