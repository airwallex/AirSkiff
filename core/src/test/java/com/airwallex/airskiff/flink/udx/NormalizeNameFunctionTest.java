package com.airwallex.airskiff.flink.udx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NormalizeNameFunctionTest {
  NormalizeNameFunction f = new NormalizeNameFunction();

  @Test
  public void testNameWithNull() {
    String name = f.eval(null);
    Assertions.assertEquals("", name);
  }

  @Test
  public void testNameWithUpperCase() {
    String name = f.eval("ABc");
    Assertions.assertEquals("abc", name);
  }

  @Test
  public void testNameWithSpace() {
    String name = f.eval("ABc Def");
    Assertions.assertEquals("abcdef", name);
  }
}
