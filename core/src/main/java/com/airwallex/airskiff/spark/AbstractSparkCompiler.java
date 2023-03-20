package com.airwallex.airskiff.spark;

import com.airwallex.airskiff.Compiler;
import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.core.*;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.api.Window;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udaf;

public class AbstractSparkCompiler implements Compiler<Dataset<?>> {

  private final SparkSession sparkSession;

  public AbstractSparkCompiler(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }


  @Override
  public <T> Dataset compile(Stream<T> stream) {
    if (stream instanceof SourceStream) {
      return ((SparkConfig<T>) ((SourceStream<T>) stream).config).dataset(sparkSession);
    }

    if (stream instanceof MapStream) {
      return compileMap((MapStream<?, T>) stream);
    }

    if (stream instanceof FlatMapStream) {
      return compileFlatMap((FlatMapStream<?, T>) stream);
    }

    if (stream instanceof SummedStream) {
      return compileSum((SummedStream<?, T>) stream);
    }

    if (stream instanceof OrderedSummedStream) {
      return compileOrderedSum((OrderedSummedStream<?, T>) stream);
    }

    if (stream instanceof ConcatStream) {
      return compileConcat((ConcatStream<T>) stream);
    }

    if (stream instanceof FilterStream) {
      return compileFilter((FilterStream<T>) stream);
    }

    if (stream instanceof LeftJoinStream) {
      return compileLeftJoin((LeftJoinStream<?, ?, T>) stream);
    }

    if (stream instanceof WindowedStream) {
      return compileWindowed((WindowedStream<?, T, ?, ?>) stream);
    }

    if (stream instanceof SqlStream) {
      return compileSql((SqlStream<T, ?>) stream);
    }

    if (stream instanceof MapValueStream) {
      return compileMapValue((MapValueStream<?, ?, T>) stream);
    }

    // order matters, as SummedOperator extends KOperator
    if (stream instanceof KStream) {
      return compileKStream((KStream<?, T>) stream);
    }

    throw new IllegalArgumentException("Unknown stream type " + stream.getClass());
  }

  private <T> Dataset compileOrderedSum(OrderedSummedStream<?, T> operator) {
    return compileSum(new SummedStream<>(operator.stream, operator.monoid));
  }

  private <K, T, U> Dataset compileMapValue(MapValueStream<K, T, U> operator) {
    Dataset<Tuple2<Long, Tuple2<K, T>>> ks1 = (Dataset<Tuple2<Long, Tuple2<K, T>>>) compile(operator.stream);
    Class<Pair<K, U>> pairClass = (Class<Pair<K, U>>) (Class<?>) Pair.class;
    Dataset<Tuple2<Long, Pair<K, U>>> dataset = ks1.map((MapFunction<Tuple2<Long, Tuple2<K, T>>, Tuple2<Long, Pair<K, U>>>) v1 -> new Tuple2<>(v1._1(), new Pair(v1._2()._1, operator.fn.apply(v1._2._2()))), Encoders.tuple(Encoders.LONG(), Utils.encode(pairClass)));
    return dataset;
  }

  private <T, U> Dataset compileSql(SqlStream<T, U> op) {
    Dataset<Tuple2<Long, T>> deserialized = compile(op.stream).map((MapFunction<Tuple2<Long, T>, Tuple2<Long, T>>) v1 -> {
//        ByteArrayInputStream is = new ByteArrayInputStream(v1);
//        ObjectInputStream ois = new ObjectInputStream(is);
//        Tuple2<Long, T> o = (Tuple2<Long, T>) ois.readObject();
        return v1;
      },
      Encoders.tuple(Encoders.LONG(), Utils.encodeSQL(op.stream.getClazz())));
    Dataset<Tuple2<Long, T>> dataset = deserialized.as(Encoders.tuple(Encoders.LONG(), Utils.encodeSQL(op.stream.getClazz())));

//    KryoSerializer serializer = new KryoSerializer(sparkSession.sparkContext().getConf());
//    String desUdfName = "kryo_deserialize_" + op.operator.getClazz().getName().replaceAll("\\.", "_");
//    Class<T> c = op.operator.getClazz();
//    Encoder<T> encoder = Encoders.bean(c);
//    StructType schema = encoder.schema();
//    UserDefinedFunction desiFn = udf(
//      (byte[] bs) -> {
////        ClassTag<T> ct = ClassManifestFactory.fromClass(op.operator.getClazz());
////        T result = serializer.newInstance().deserialize(ByteBuffer.wrap(bs), ct);
//        var bis = new ByteArrayInputStream((byte[]) bs);
//        var ois = new ObjectInputStream(bis);
////        return result;
////      return new Counter(ois.readUTF(), ois.readLong());
//        return (T) ois.readObject();
//      }, schema);
//    sparkSession.udf().register("desiPojo", desiFn);
    try {
      Dataset ds = dataset.withColumn("ts__", dataset.col("_1")).withColumn("row_time__", dataset.col("_1"));
      Field[] fields = op.stream.getClazz().getFields();
      for (Field field : fields) {
        System.out.println(field.getName());
        ds = ds.withColumn(field.getName(), ds.col("_2." + field.getName()));
      }
      // avoid duplicates
      sparkSession.catalog().dropTempView(op.tableName);
      ds.createTempView(op.tableName);
      String select = op.sql.substring(0, 6);
      String tempSql = op.sql.replaceFirst(select, select + " ts__,");
      Dataset<Row> fatResult = sparkSession.sql(tempSql);


      List<Column> cols = new ArrayList<>();
      List<String> colStrs = new ArrayList<>();
      for (String col : fatResult.columns()) {
        if (!col.equals("ts__")) {
          cols.add(new Column(col));
          colStrs.add(col);
        }
      }

      Column[] cc = new Column[cols.size()];
      cols.toArray(cc);
      Dataset<Row> fatDs = fatResult.withColumn("data", struct(cc));


      for (String col : colStrs) {
        fatDs = fatDs.drop(col);
      }

      fatDs = fatDs.drop("row_time__");
      fatDs = fatDs.withColumnRenamed("data", "_2");
      fatDs = fatDs.withColumnRenamed("ts__", "_1");

      fatDs.printSchema();
      fatDs.show();


      Encoder<U> encoder = Utils.encodeSQL(op.tc);
      Dataset<Tuple2<Long, U>> singleDs = fatDs.as(Encoders.tuple(Encoders.LONG(), encoder));
      singleDs.printSchema();
      singleDs.show();

      return singleDs;


//      Dataset<Tuple2<Long, U>> dd = fatResult.as(Encoders.tuple(Encoders.LONG(), Utils.encode(op.tc)));
//      dd.show();
//      dd.printSchema();

//      Encoder<U> encoder = Encoders.bean(op.tc);
//      ExpressionEncoder<U> enc = ExpressionEncoder.javaBean(op.tc);

//      return dd;
//      return fatResult.as(Encoders.tuple(Encoders.LONG(), Utils.encode(op.tc)));
//      Dataset<Tuple2<Long, U>> finalDs = fatResult.map((MapFunction<Row, Tuple2<Long, U>>) row -> {
//        try {
//          U u = (U) op.operator.getClazz().getConstructor().newInstance();
//          for (Field field : fields) {
//            field.set(u, row.getAs(field.getName()));
//          }
//          return new Tuple2<>(row.getAs("ts__"), u);
//        } catch (Exception e) {
//          throw new RuntimeException(e);
//        }
//      }, Encoders.tuple(Encoders.LONG(), Utils.encode(op.tc)));
//      return fatResult;
//      return finalDs;
    } catch (AnalysisException e) {
      throw new RuntimeException(e);
    }
  }

  private <K, T, U, W extends Window> Dataset compileWindowed(WindowedStream<K, T, U, W> op) {
    Dataset<Tuple2<Long, Tuple2<K, T>>> ds = compile(op.stream);
    Encoder<Tuple3<Long, K, T>> expandedEncoder = Encoders.tuple(Encoders.LONG(), Utils.encode(op.keyClass()), Utils.encode(StreamUtils.kStreamClass(op.stream)));
    Dataset<Tuple3<Long, K, T>> expanded = ds.map((MapFunction<Tuple2<Long, Tuple2<K, T>>, Tuple3<Long, K, T>>) t -> {
      return new Tuple3<>(t._1(), t._2()._1(), t._2()._2());
    }, expandedEncoder);


    Dataset<Row> rowDs = expanded.withColumnRenamed("_1", "ts").withColumnRenamed("_2", "key").withColumnRenamed("_3", "value");
    final Window w = op.window;

    // unsupported window
    if (!(w instanceof EventTimeBasedSlidingWindow)) {
      throw new IllegalArgumentException("window type not supported: " + w.getClass().getName());
    }

    EventTimeBasedSlidingWindow window = (EventTimeBasedSlidingWindow) w;
    long size = window.size().toMillis() + window.slide().toMillis();
//    org.apache.spark.sql.expressions.WindowSpec windowSpec = org.apache.spark.sql.expressions.Window.
//      partitionBy("key").
//      orderBy("ts").
//      rangeBetween(-size, 0);


    Class<T> inClz = StreamUtils.kStreamClass(op.stream);
    Class<U> outClz = op.uc;
    CustomAggregator<T, U> agg = new CustomAggregator<>(op.f, op.uc);

//    Column aggCol = agg.toColumn().apply(finalDs.col("value")).over(windowSpec).as("agg_result");
//    finalDs.select(aggCol).show();


    sparkSession.udf().register("riskyAgg", udaf(agg, Utils.encode(inClz)));
    String tempTableName = "windowedTempTable";
    try {
      sparkSession.catalog().dropTempView(tempTableName);
      rowDs.createTempView(tempTableName);
      Dataset<Row> sqlResult = sparkSession.sql("select ts, key, riskyAgg(value.*) over (PARTITION BY key ORDER BY ts RANGE BETWEEN " + size + " PRECEDING AND CURRENT ROW) as agg_result from " + tempTableName);
      Dataset<Tuple3<Long, K, U>> typedDs = sqlResult.as(Encoders.tuple(Encoders.LONG(), Utils.encode(op.keyClass()), Utils.encode(op.uc)));
      Class<Pair<K, U>> pairClass2 = (Class<Pair<K, U>>) new Pair<K, U>().getClass();
      Dataset<Tuple2<Long, Pair<K, U>>> finalResult = typedDs.map((MapFunction<Tuple3<Long, K, U>, Tuple2<Long, Pair<K, U>>>) t -> {
        return new Tuple2<>(t._1(), new Pair<>(t._2(), t._3()));
      }, Encoders.tuple(Encoders.LONG(), Utils.encode(pairClass2)));
      return finalResult;
    } catch (AnalysisException e) {
      throw new RuntimeException(e);
    }

  }

  private <K, T, U> Dataset compileLeftJoin(LeftJoinStream op) {
    Map<K, Pair<T, U>> accState = new HashMap<>();
    Dataset<Tuple2<Long, Tuple2<K, T>>> ks1 = (Dataset<Tuple2<Long, Tuple2<K, T>>>) compile(op.s1);
    Dataset<Tuple2<Long, Tuple2<K, U>>> ks2 = (Dataset<Tuple2<Long, Tuple2<K, U>>>) compile(op.s2);

    System.out.println("ks1:");
    ks1.show(false);

    System.out.println("ks2:");
    ks2.show(false);
    Class<K> kClass = op.s1.keyClass();

    Class<Pair<T, U>> pairClass = (Class<Pair<T, U>>) new Pair<T, U>().getClass();
    // without Java Serialization this fails
    Encoder<Tuple2<Long, Tuple2<K, Pair<T, U>>>> encoder4 = Encoders.tuple(Encoders.LONG(), Encoders.tuple(Utils.encode(kClass), Utils.encode(pairClass)));
    Class<KeyedItem<K, T, U>> kiClz = (Class<KeyedItem<K, T, U>>) new KeyedItem<>().getClass();

    // we have to use java serialization in order to solve the serialization problem
    Dataset<KeyedItem<K, T, U>> expanded1 = ks1.map((MapFunction<Tuple2<Long, Tuple2<K, T>>, KeyedItem<K, T, U>>) t -> {
      return new KeyedItem<>(t._1(), t._2()._1(), t._2()._2(), null);
    }, Encoders.javaSerialization(kiClz));

    // as mentioned above
    Dataset<KeyedItem<K, T, U>> expanded2 = ks2.map((MapFunction<Tuple2<Long, Tuple2<K, U>>, KeyedItem<K, T, U>>) t -> {
      return new KeyedItem<>(t._1(), t._2()._1(), null, t._2()._2());
    }, Encoders.javaSerialization(kiClz));

    Dataset<KeyedItem<K, T, U>> holyUnion = expanded2.union(expanded1);
    holyUnion.show();
    holyUnion.printSchema();

    LeftJoinPairMonoid<T, U> leftJoinMonoid = new LeftJoinPairMonoid<T, U>();
    Class<Pair<K, Pair<T, U>>> pairPairClass = (Class<Pair<K, Pair<T, U>>>) (Class<?>) new Pair<K, Pair<T, U>>().getClass();
    Class<SerializableList<KeyedItem<K, T, U>>> listClz = (Class<SerializableList<KeyedItem<K, T, U>>>) new SerializableList<KeyedItem<K, T, U>>().getClass();
    Dataset<SerializableList<KeyedItem<K, T, U>>> groupResult = holyUnion.groupByKey((MapFunction<KeyedItem<K, T, U>, K>) t -> t.getKey(), Encoders.javaSerialization(kClass)).mapGroups((MapGroupsFunction<K, KeyedItem<K, T, U>, SerializableList<KeyedItem<K, T, U>>>) (k, v) -> {
      SerializableList<KeyedItem<K, T, U>> result = new SerializableList<>();
      final Pair<T, U> state = new Pair<>(null, null);
      v.forEachRemaining(t -> {
        Pair<T, U> temp;
        Pair<T, U> newPair = new Pair<>(t.getVal1(), t.getVal2());
        if (state.getL() == null && state.getR() == null) {
          temp = leftJoinMonoid.plus(null, newPair);
        } else {
          temp = leftJoinMonoid.plus(state, newPair);
        }
        state.setL(temp.getL());
        state.setR(temp.getR());
        t.setVal1(state.getL());
        t.setVal2(state.getR());
        result.getList().add(t);
      });
      return result;
    }, Encoders.javaSerialization(listClz));

    groupResult.printSchema();
    groupResult.show();

    Dataset<Tuple2<Long, Pair<K, Pair<T, U>>>> finalResult = groupResult.flatMap((FlatMapFunction<SerializableList<KeyedItem<K, T, U>>, Tuple2<Long, Pair<K, Pair<T, U>>>>) t -> {
      ArrayList<Tuple2<Long, Pair<K, Pair<T, U>>>> result = new ArrayList<>();
      t.getList().forEach(item -> {
        result.add(new Tuple2<>(item.getTs(), new Pair<>(item.getKey(), new Pair<>(item.getVal1(), item.getVal2()))));
      });
      return result.iterator();
    }, Encoders.tuple(Encoders.LONG(), Utils.encode(pairPairClass)));


    finalResult = finalResult.filter((FilterFunction<Tuple2<Long, Pair<K, Pair<T, U>>>>) t -> t._2.r.l != null);
    System.out.println(finalResult.count());

    return finalResult;
  }

  private <T> Dataset<Tuple2<Long, T>> compileFilter(FilterStream<T> op) {
    Encoder<Tuple2<Long, T>> encoders = Encoders.tuple(Encoders.LONG(), Utils.encode(op.stream.getClazz()));
    Dataset<Tuple2<Long, T>> ds = compile(op.stream).as(encoders);
    return ds.filter((FilterFunction<Tuple2<Long, T>>) t -> op.p.apply(t._2));
  }

  private <T> Dataset<Tuple2<Long, T>> compileConcat(ConcatStream<T> operator) {
    return compile(operator.a).unionAll(compile(operator.b));
  }

  private <K, T> Dataset<Tuple2<Long, Pair<K, T>>> compileSum(SummedStream<K, T> op) {
    Class<Pair<Long, T>> pairClass = (Class<Pair<Long, T>>) new Pair<Long, T>().getClass();
    Class<T> cc = StreamUtils.kStreamClass(op.stream);
    Dataset<Tuple2<Long, Tuple2<K, T>>> data = (Dataset<Tuple2<Long, Tuple2<K, T>>>) compile(op.stream).as(Encoders.tuple(Encoders.LONG(), Encoders.tuple(Utils.encode(op.keyClass()), Utils.encode(cc))));
    Dataset<Tuple2<K, Tuple2<Long, T>>> grouped = data.map((MapFunction<Tuple2<Long, Tuple2<K, T>>, Tuple2<K, Tuple2<Long, T>>>) t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)), Encoders.tuple(Utils.encode(op.keyClass()), Encoders.tuple(Encoders.LONG(), Utils.encode(cc))));

    Dataset<Tuple2<K, Tuple2<K, Tuple2<Long, T>>>> summed = grouped.groupByKey((MapFunction<Tuple2<K, Tuple2<Long, T>>, K>) Tuple2::_1, Utils.encode(op.keyClass())).reduceGroups((ReduceFunction<Tuple2<K, Tuple2<Long, T>>>) (t1, t2) -> {
      Long ts1 = (Long) t1._2._1;
      Long ts2 = (Long) t2._2._1;
      return new Tuple2<>(t1._1, new Tuple2<>(Math.max(ts1, ts2), op.monoid.plus(t1._2._2, t2._2._2)));
    });
    Class<Pair<K, T>> pairClass2 = (Class<Pair<K, T>>) new Pair<K, T>().getClass();
    Dataset<Tuple2<Long, Pair<K, T>>> ds = summed.map((MapFunction<Tuple2<K, Tuple2<K, Tuple2<Long, T>>>, Tuple2<Long, Pair<K, T>>>) t -> new Tuple2<>(t._2._2._1, new Pair<K, T>(t._2._1, t._2._2._2)), Encoders.tuple(Encoders.LONG(), Utils.encode(pairClass2)));

//    Dataset<Tuple2<K, Tuple2<K, T>>> summed = grouped.groupByKey((MapFunction<Tuple2<K, Pair<Long, T>>, K>>) Tuple2::_1, Utils.encode(op.keyClass()))
//      .reduceGroups((ReduceFunction<Tuple2<K, T>>) (t1, t2) -> new Tuple2<>(t1._1, op.monoid.plus(t1._2, t2._2)));
//    return summed.map((MapFunction<Tuple2<K, Tuple2<K, T>>, Tuple2<K, T>>) t -> t._2(),
//      Encoders.tuple(Utils.encode(op.keyClass()), Utils.encode(StreamUtils.kStreamClass(op.stream))));
    return ds;
  }

  private <T, U> Dataset<Tuple2<Long, U>> compileFlatMap(FlatMapStream<T, U> op) {
    return compile(op.stream).flatMap((FlatMapFunction<Tuple2<Long, T>, Tuple2<Long, U>>) t -> {
      List<Tuple2<Long, U>> results = new ArrayList<>();
      op.f.apply(t._2).forEach(o -> {
        results.add(new Tuple2<>(t._1, o));
      });
      return results.iterator();
    }, Encoders.tuple(Encoders.LONG(), Utils.encode(op.getClazz())));
  }


  private <T, U> Dataset<Tuple2<Long, U>> compileMap(MapStream<T, U> op) {
    return compile(op.stream).map((MapFunction<Tuple2<Long, T>, Tuple2<Long, U>>) t -> new Tuple2<>(t._1, op.f.apply(t._2)), Encoders.tuple(Encoders.LONG(), Utils.encode(op.getClazz())));
  }


  private <K, T> Dataset<Tuple2<Long, Tuple2<K, T>>> compileKStream(KStream<?, T> op) {
    if (op instanceof KeyedSimpleStream) {
      KeyedSimpleStream<K, T> kso = (KeyedSimpleStream<K, T>) op;
      Dataset<Tuple2<Long, Tuple2<K, T>>> result = compile(kso.stream).map((MapFunction<Tuple2<Long, T>, Tuple2<Long, Tuple2<K, T>>>) t -> new Tuple2<>(t._1, new Tuple2<>(kso.toKey.apply(t._2), t._2)), Encoders.tuple(Encoders.LONG(), Encoders.tuple(Utils.encode(kso.kc), Utils.encode(StreamUtils.clz(kso.stream)))));
      return result;
    }
    throw new UnsupportedOperationException("Unknown KOperator " + op.getClass());
  }

}
