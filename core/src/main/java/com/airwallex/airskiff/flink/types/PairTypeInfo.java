package com.airwallex.airskiff.flink.types;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.airwallex.airskiff.common.Pair;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class PairTypeInfo<L, R> extends TypeInformation<Pair<L, R>> {
  private final TypeInformation<L> leftType;
  private final TypeInformation<R> rightType;

  public PairTypeInfo(TypeInformation<L> leftType, TypeInformation<R> rightType) {
    this.leftType = checkNotNull(leftType);
    this.rightType = checkNotNull(rightType);
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 2;
  }

  @Override
  public int getTotalFields() {
    return 2;
  }

  @Override
  public Class<Pair<L, R>> getTypeClass() {
    return (Class<Pair<L, R>>) (Class<?>) Pair.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<Pair<L, R>> createSerializer(ExecutionConfig config) {
    return new PairSerializer<L, R>(leftType.createSerializer(config), rightType.createSerializer(config));
  }

  @Override
  public String toString() {
    return "Pair <" + leftType.toString() + ", " + rightType.toString() + ">";
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PairTypeInfo) {
      PairTypeInfo<L, R> other = (PairTypeInfo<L, R>) o;

      return other.canEqual(this) && leftType.equals(other.leftType) && rightType.equals(other.rightType);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 17 * leftType.hashCode() + rightType.hashCode();
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof PairTypeInfo;
  }
}
