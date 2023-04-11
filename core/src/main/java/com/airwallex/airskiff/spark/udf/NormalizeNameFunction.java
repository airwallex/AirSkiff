package com.airwallex.airskiff.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

public class NormalizeNameFunction implements UDF1<String, String> {

  @Override
  public String call(String name) throws Exception {
    if (null == name) {
      return "";
    }
    return name.toLowerCase().replace(" ", "");
  }
}
