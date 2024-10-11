package com.airwallex.airskiff.flink;

import com.airwallex.airskiff.Compiler;
import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.functions.NamedSerializableIterableLambda;
import com.airwallex.airskiff.core.*;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.api.Window;
import com.airwallex.airskiff.flink.types.PairTypeInfo;
import com.google.api.client.util.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;

import static com.airwallex.airskiff.flink.Utils.*;
import static org.apache.flink.table.api.Expressions.$;

public abstract class AbstractFlinkCompiler implements Compiler<DataStream<?>> {
  protected final StreamExecutionEnvironment env;
  protected final StreamTableEnvironment tableEnv;
  protected final Duration allowedLatency;
  protected final Duration withIdleness;

  public AbstractFlinkCompiler(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, Duration allowedLatency,
                               Duration withIdleness) {
    this.env = env;
    this.tableEnv = tableEnv;
    this.allowedLatency = allowedLatency;
    this.withIdleness = withIdleness;
    Utils.registerFunctions(this.tableEnv);
  }

  protected abstract boolean isBatch();

  @Override
  public <T> DataStream<Tuple2<Long, T>> compile(Stream<T> stream) {
    if (stream instanceof SourceStream) {
      return ((FlinkConfig<T>) ((SourceStream<T>) stream).config).source(env, isBatch());
    }
    if (stream instanceof KStream) {
      return (DataStream<Tuple2<Long, T>>) (DataStream) compileKS((KStream<?, ?>) stream);
    }
    if (stream instanceof FlatMapStream) {
      return compileFlat((FlatMapStream<?, T>) stream);
    }
    if (stream instanceof MapStream) {
      return compileMap((MapStream<?, T>) stream);
    }

    if (stream instanceof ConcatStream) {
      return compileConcat((ConcatStream<T>) stream);
    }
    if (stream instanceof SqlStream) {
      return compileSql((SqlStream<?, T>) stream);
    }
    if (stream instanceof FilterStream) {
      return compileFilter((FilterStream<T>) stream);
    }
    throw new IllegalArgumentException("Unknown stream type " + stream.getClass());
  }

  protected <K, T, U> KeyedStream<Tuple2<Long, Pair<K, Pair<T, U>>>, K> compileLeftJoin(LeftJoinStream<K, T, U> stream) {
    KeyedStream<Tuple2<Long, Pair<K, T>>, K> ks1 = compileKS(stream.s1);
    KeyedStream<Tuple2<Long, Pair<K, U>>, K> ks2 = compileKS(stream.s2);
    final TypeInformation<K> kType = typeInfo(stream.keyClass());
    final TypeInformation<Pair<T, U>> pairType = pairType(stream);
    final TypeInformation<Pair<K, Pair<T, U>>> keyedPairType = new PairTypeInfo<>(kType, pairType);
    final TypeInformation<Tuple2<Long, Pair<K, Pair<T, U>>>> outputType = new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, keyedPairType);

    final LeftJoinPairMonoid<T, U> m = new LeftJoinPairMonoid<>();
    DataStream<Tuple2<Long, Pair<K, Pair<T, U>>>> ss = ks2.map(x -> new Tuple2<>(x.f0, new Pair<>(x.f1.l, new Pair<T, U>(null, x.f1.r))), outputType).union(ks1.map(x -> new Tuple2<>(x.f0, new Pair<>(x.f1.l, new Pair<T, U>(x.f1.r, null))), outputType));
    return ss.assignTimestampsAndWatermarks(Utils.watermark(isBatch(), Duration.ZERO, Duration.ofMillis(300))).keyBy(t -> t.f1.l, kType)
      // a window to make sure if we have multiple events happening
      // at the same time, U is always put before T in batch mode
      .window(TumblingEventTimeWindows.of(Time.days(1))).allowedLateness(Time.days(1)).process(new ProcessWindowFunction<Tuple2<Long, Pair<K, Pair<T, U>>>, Tuple2<Long, Pair<K, Pair<T, U>>>, K, TimeWindow>() {
        // This is a global state per key across windows
        private ValueState<Pair<T, U>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
          state = getRuntimeContext().getState(new ValueStateDescriptor<>("pairState", pairType));
        }

        @Override
        public void process(K k, Context context, Iterable<Tuple2<Long, Pair<K, Pair<T, U>>>> iterable, Collector<Tuple2<Long, Pair<K, Pair<T, U>>>> collector) throws Exception {
          List<Tuple2<Long, Pair<K, Pair<T, U>>>> l = new ArrayList<>();
          for (Tuple2<Long, Pair<K, Pair<T, U>>> t : iterable) {
            l.add(t);
          }
          l.sort(new Comparator<Tuple2<Long, Pair<K, Pair<T, U>>>>() {
            @Override
            public int compare(Tuple2<Long, Pair<K, Pair<T, U>>> o1, Tuple2<Long, Pair<K, Pair<T, U>>> o2) {
              int res = o1.f0.compareTo(o2.f0);
              if (res != 0) {
                return res;
              }
              if (o1.f1.r.r != null && o2.f1.r.r != null) {
                return 0;
              }
              if (o1.f1.r.r != null) {
                return -1;
              } else {
                return 1;
              }
            }
          });
          Pair<T, U> cur = state.value();
          for (Tuple2<Long, Pair<K, Pair<T, U>>> t : l) {
            cur = m.plus(cur, t.f1.r);
            state.update(cur);
            collector.collect(new Tuple2<>(t.f0, new Pair<>(t.f1.l, cur)));
          }
        }
      }, outputType).filter(t -> t.f1.r.l != null).keyBy(t -> t.f1.l, kType);
  }

  protected <T, U> DataStream<Tuple2<Long, U>> compileFlat(FlatMapStream<T, U> fms) {
    return compile(fms.stream).flatMap((FlatMapFunction<Tuple2<Long, T>, Tuple2<Long, U>>) (t, collector) -> {
      fms.f.apply(t.f1).forEach(o -> collector.collect(new Tuple2<>(t.f0, o)));
    }, tuple2TypeInfo(fms.uc));
  }

  protected <T, U> DataStream<Tuple2<Long, U>> compileMap(MapStream<T, U> ms) {
    return compile(ms.stream).map(t -> new Tuple2<>(t.f0, ms.f.apply(t.f1)), tuple2TypeInfo(ms.uc));
  }

  protected <K, T> KeyedStream<Tuple2<Long, Pair<K, T>>, K> compileSum(SummedStream<K, T> stream) {
    final TypeInformation<T> info = typeInfo(StreamUtils.kStreamClass(stream));
    final TypeInformation<Pair<K, T>> keyedPairType = new PairTypeInfo<>(typeInfo(stream.keyClass()), info);
    final TypeInformation<Tuple2<Long, Pair<K, T>>> outputType = new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, keyedPairType);
    KeyedStream<Tuple2<Long, Pair<K, T>>, K> ks = compileKS(stream.stream);
    return new KeyedStream<>(ks.map(new RichMapFunction<Tuple2<Long, Pair<K, T>>, Tuple2<Long, Pair<K, T>>>() {
      private ValueState<T> total;

      @Override
      public Tuple2<Long, Pair<K, T>> map(Tuple2<Long, Pair<K, T>> t) throws Exception {
        T currentTotal = total.value();
        T next = stream.monoid.plus(currentTotal, t.f1.r);
        total.update(next);
        return new Tuple2<>(t.f0, new Pair<>(t.f1.l, next));
      }

      @Override
      public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stream.monoid.name(), info, stream.monoid.zero());
        total = getRuntimeContext().getState(descriptor);
      }
    }, outputType), t -> t.f1.l);
  }

  protected <K, T> KeyedStream<Tuple2<Long, Pair<K, T>>, K> compileOrderedSum(OrderedSummedStream<K, T> stream) {
    return compileSum(new SummedStream<>(stream.stream, stream.monoid));
  }

  protected <T> DataStream<Tuple2<Long, T>> compileConcat(ConcatStream<T> stream) {
    return compile(stream.a).union(compile(stream.b))
      .assignTimestampsAndWatermarks(Utils.watermark(isBatch(), this.allowedLatency, this.withIdleness));
  }

  protected <T> DataStream<Tuple2<Long, T>> compileFilter(FilterStream<T> stream) {
    return compile(stream.stream).filter(t -> stream.p.apply(t.f1));
  }

  protected <T, U> DataStream<Tuple2<Long, U>> compileSql(final SqlStream<T, U> stream) {
    final var table = createSqlTable(stream.tableName, stream.sql, stream.stream);
    final var mapper = stream.mapper;
    final var typeInfo = stream.typeInfo;
    return tableEnv.toAppendStream(table, Row.class).map(r -> new Tuple2<>((Long) r.getField(0), mapper.map(r)), typeInfo)
      // watermark and timestamp is lost after table to data stream conversion?
      .assignTimestampsAndWatermarks(Utils.watermark(isBatch(), this.allowedLatency, this.withIdleness));
  }

  protected <K, T> KeyedStream<Tuple2<Long, Pair<K, T>>, K> compileKS(KStream<K, T> ks) {
    if (ks instanceof KeyedSimpleStream) {
      KeyedSimpleStream<K, T> s = (KeyedSimpleStream<K, T>) ks;
      return new KeyedStream<>(compile(s.stream).map(t -> new Tuple2<>(t.f0, new Pair<>(s.toKey.apply(t.f1), t.f1)), new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, new PairTypeInfo<>(typeInfo(s.kc), typeInfo(s.stream.getClazz())))), t -> t.f1.l, typeInfo(s.kc));
    }
    if (ks instanceof LeftJoinStream) {
      return (KeyedStream<Tuple2<Long, Pair<K, T>>, K>) (KeyedStream) compileLeftJoin((LeftJoinStream<K, ?, ?>) ks);
    }
    if (ks instanceof SummedStream) {
      return compileSum((SummedStream<K, T>) ks);
    }
    if (ks instanceof OrderedSummedStream) {
      return compileOrderedSum((OrderedSummedStream<K, T>) ks);
    }
    if (ks instanceof MapValueStream) {
      return mapValue((MapValueStream<K, ?, T>) ks);
    }
    if (ks instanceof WindowedStream) {
      return compileWindow((WindowedStream<K, ?, T, ? extends Window>) ks);
    }
    throw new IllegalArgumentException("Unknown KStream type: " + ks.getClass());
  }

  private <K, T, U> KeyedStream<Tuple2<Long, Pair<K, U>>, K> mapValue(MapValueStream<K, T, U> ks) {
    KeyedStream<Tuple2<Long, Pair<K, T>>, K> s = compileKS(ks.stream);
    return new KeyedStream<>(s.map(t -> new Tuple2<>(t.f0, new Pair<>(t.f1.l, ks.fn.apply(t.f1.r))), new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, new PairTypeInfo<>(typeInfo(ks.keyClass()), typeInfo(ks.uc)))), t -> t.f1.l);
  }

  protected <K, T, U, W extends Window> KeyedStream<Tuple2<Long, Pair<K, U>>, K> compileWindow(WindowedStream<K, T, U, W> stream) {
    KeyedStream<Tuple2<Long, Pair<K, T>>, K> ks = compileKS(stream.stream);
    final Window w = stream.window;
    final Class<T> clz = StreamUtils.kStreamClass(stream.stream);
    final NamedSerializableIterableLambda<T, U> f = stream.f;
    if (w instanceof EventTimeBasedSlidingWindow) {
      final EventTimeBasedSlidingWindow sw = (EventTimeBasedSlidingWindow) w;
      return new KeyedStream<>(ks.process(new KeyedProcessFunction<K, Tuple2<Long, Pair<K, T>>, Tuple2<Long, Pair<K, U>>>() {
        private transient MapState<Long, T> elementMap;

        public void open(Configuration parameters) throws Exception {
          long ttl = sw.size().toSeconds() + sw.slide().toSeconds() * 14;
          StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(org.apache.flink.api.common.time.Time.seconds(ttl))
            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupIncrementally(10, false)
            .build();
          MapStateDescriptor<Long, T> descriptor = new MapStateDescriptor<>("elements", BasicTypeInfo.LONG_TYPE_INFO, typeInfo(clz));
          descriptor.enableTimeToLive(ttlConfig);
          elementMap = getRuntimeContext().getMapState(descriptor);
        }

        private void updateMap(Long timestamp, T value) throws Exception {
          long lowerBoundInclusive = timestamp - sw.size().toMillis() - sw.slide().toMillis() * 14;

          // Remove old elements
          Iterator<Map.Entry<Long, T>> iterator = elementMap.entries().iterator();
          while (iterator.hasNext()) {
            Map.Entry<Long, T> entry = iterator.next();
            if (entry.getKey() < lowerBoundInclusive) {
              iterator.remove();
            }
          }

          // Add new element
          elementMap.put(timestamp, value);
        }

        @Override
        public void processElement(Tuple2<Long, Pair<K, T>> tuple, Context context, Collector<Tuple2<Long, Pair<K, U>>> collector) throws Exception {
          Long timestamp = tuple.f0;
          T value = tuple.f1.r;

          updateMap(timestamp, value);

          List<T> windowElements = new ArrayList<>();
          long windowStart = timestamp - sw.size().toMillis();

          for (Map.Entry<Long, T> entry : elementMap.entries()) {
            if (entry.getKey() >= windowStart && entry.getKey() <= timestamp) {
              windowElements.add(entry.getValue());
            }
          }

          List<U> results = Lists.newArrayList(f.apply(windowElements));
          if (!results.isEmpty()) {
            U last = results.get(results.size() - 1);
            collector.collect(new Tuple2<>(timestamp, new Pair<>(tuple.f1.l, last)));
          }
        }
      }, new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, new PairTypeInfo<>(typeInfo(stream.keyClass()), typeInfo(stream.uc)))), t -> t.f1.l);
    }
    throw new IllegalArgumentException("window type not supported: " + w.getClass().getName());
  }

  protected <T> Table createSqlTable(String tableName, String sql, Stream<T> stream) {
    DataStream<Tuple2<Long, T>> ds = compile(stream);
    String tempTableName = tableName + "_temp_" + RandomStringUtils.randomAlphabetic(5);
    tableEnv.createTemporaryView(tempTableName, ds, $("f0"), $("f1"), $("f0").rowtime().as("f2"));
    Class<T> tc = StreamUtils.clz(stream);
    Field[] fds = StreamUtils.getFields(tc);
    StringBuilder expandSqlBuilder = new StringBuilder("SELECT f0 AS ts__, f2 AS row_time__,");
    for (Field fd : fds) {
      String fn = fd.getName();
      expandSqlBuilder.append(" f1.");
      expandSqlBuilder.append(fn);
      expandSqlBuilder.append(" AS ");
      expandSqlBuilder.append(fn);
      expandSqlBuilder.append(",");
    }
    // remove the last comma
    expandSqlBuilder.deleteCharAt(expandSqlBuilder.length() - 1);
    expandSqlBuilder.append(" FROM ");
    expandSqlBuilder.append(tempTableName);

    Table table = tableEnv.sqlQuery(expandSqlBuilder.toString());
    tableEnv.createTemporaryView(tableName, table);
    String select = sql.substring(0, 6);
    // f0 is a reserved field for our internal timestamp
    String tempSql = sql.replaceFirst(select, select + " ts__,");
    Table t = tableEnv.sqlQuery(tempSql);
    tableEnv.dropTemporaryView(tempTableName);
    tableEnv.dropTemporaryView(tableName);
    return t;
  }
}
