package com.airwallex.airskiff.flink.udx;

import org.apache.flink.table.functions.ScalarFunction;

public class NormalizeNameFunction extends ScalarFunction {
  public static final String name = "NormalizeName";

  public String eval(String name) {
    if (null == name) {
      return "";
    }
    return name.toLowerCase().replace(" ", "");
  }
}
