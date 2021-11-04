package com.airwallex.airskiff.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkRealtimeCompiler extends AbstractFlinkCompiler {

  public FlinkRealtimeCompiler(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    super(env, tableEnv);
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
  }

  @Override
  protected boolean isBatch() {
    return false;
  }
}
