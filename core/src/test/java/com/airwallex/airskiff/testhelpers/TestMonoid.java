package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.common.functions.NamedMonoid;

public class TestMonoid implements NamedMonoid<TestInputData> {
  @Override
  public TestInputData plus(TestInputData o1, TestInputData o2) {
    return new TestInputData(o1.a + o2.a, o2.b);
  }

  @Override
  public TestInputData zero() {
    return new TestInputData(0);
  }
}
