package com.airwallex.airskiff.core;

import java.io.Serializable;

/**
 * A Either type that holds one of the two values. Caller must first use isLeft() to decide whether
 * it is a Left or a Right. Calling the wrong side would throw exceptions. TODO: check if we want to
 * use Either from https://www.vavr.io
 */
public abstract class Either<L, R> implements Serializable {
  public abstract L left();

  public abstract R right();

  public abstract boolean isLeft();

  public static class Left<L, R> extends Either<L, R> {
    private final L l;

    public Left(L l) {
      this.l = l;
    }

    @Override
    public L left() {
      return l;
    }

    @Override
    public R right() {
      throw new RuntimeException("Shouldn't call right() on a Either.Left");
    }

    @Override
    public boolean isLeft() {
      return true;
    }
  }

  public static class Right<L, R> extends Either<L, R> {

    private final R r;

    public Right(R r) {
      this.r = r;
    }

    @Override
    public L left() {
      throw new RuntimeException("Shouldn't call left() on a Either.Right");
    }

    @Override
    public R right() {
      return r;
    }

    @Override
    public boolean isLeft() {
      return false;
    }
  }
}
