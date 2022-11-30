package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.Compiler;
import com.airwallex.airskiff.common.Pair;
import com.airwallex.airskiff.common.SlidingWindowList;
import com.airwallex.airskiff.common.functions.NamedMonoid;
import com.airwallex.airskiff.common.functions.NamedSerializableIterableLambda;
import com.airwallex.airskiff.core.*;
import com.airwallex.airskiff.core.api.KStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.core.api.Window;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class TestCompiler implements Compiler<List<?>> {
  private static final Map<Class<?>, Class<?>> primitiveMap;
  private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

  static {
    primitiveMap = new HashMap<>(9);
    primitiveMap.put(int.class, Integer.class);
    primitiveMap.put(byte.class, Byte.class);
    primitiveMap.put(char.class, Character.class);
    primitiveMap.put(boolean.class, Boolean.class);
    primitiveMap.put(double.class, Double.class);
    primitiveMap.put(float.class, Float.class);
    primitiveMap.put(long.class, Long.class);
    primitiveMap.put(short.class, Short.class);
    primitiveMap.put(void.class, Void.class);
  }

  private final String h2Url;

  public TestCompiler() {
    var home = System.getenv("HOME");
    h2Url = "jdbc:h2:" + home + "/h2_test_01";
    try (Connection conn = DriverManager.getConnection(h2Url)) {
      String sql =
        "CREATE ALIAS IF NOT EXISTS UnixTime FOR \"com.airwallex.airskiff.testhelpers.TestCompiler.date\"; ";
      Statement statement = conn.createStatement();
      statement.execute(sql);
    } catch (Exception e) {

    }
  }

  // H2 registered function must be public static
  public static String date(long tsInMillis, String format) {
    return Instant.ofEpochMilli(tsInMillis).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(format));
  }

  @Override
  public <T> List<Pair<Long, T>> compile(Stream<T> stream) {

    if (stream instanceof SourceStream) {
      return ((com.airwallex.airskiff.testhelpers.ListSourceConfig<T>) ((SourceStream<T>) stream).config).source();
    }
    if (stream instanceof MapStream) {
      return compileMap((MapStream<?, T>) stream);
    }
    if (stream instanceof FilterStream) {
      return compileFilter((FilterStream<T>) stream);
    }
    if (stream instanceof ConcatStream) {
      return compileConcat((ConcatStream<T>) stream);
    }
    if (stream instanceof FlatMapStream) {
      return compileFlat((FlatMapStream<?, T>) stream);
    }
    if (stream instanceof KStream) {
      return (List<Pair<Long, T>>) (List) compileKS((KStream<?, ?>) stream);
    }
    if (stream instanceof SqlStream) {
      return compileSql((SqlStream<?, T>) stream);
    }
    throw new IllegalArgumentException("Unknown stream type " + stream.getClass());
  }

  private <K, T, U> KList<K, Pair<Long, Pair<K, Pair<T, U>>>> compileLeftJoin(
    LeftJoinStream<K, T, U> stream
  ) {
    List<Pair<Long, Pair<K, T>>> l1 = compileKS(stream.s1);
    List<Pair<Long, Pair<K, U>>> l2 = compileKS(stream.s2);

    List<Pair<Long, Pair<K, Pair<T, U>>>> l3 = new ArrayList<>();
    for (Pair<Long, Pair<K, T>> p : l1) {
      l3.add(new Pair<>(p.l, new Pair<>(p.r.l, new Pair<>(p.r.r, null))));
    }
    for (Pair<Long, Pair<K, U>> p : l2) {
      l3.add(new Pair<>(p.l, new Pair<>(p.r.l, new Pair<>(null, p.r.r))));
    }

    l3.sort(new Comparator<Pair<Long, Pair<K, Pair<T, U>>>>() {
      @Override
      public int compare(Pair<Long, Pair<K, Pair<T, U>>> o1, Pair<Long, Pair<K, Pair<T, U>>> o2) {
        int res = o1.l.compareTo(o2.l);
        if (res != 0) {
          return res;
        }
        if (o1.r.r.r != null && o2.r.r.l != null) {
          return -1;
        }
        if (o1.r.r.l != null && o2.r.r.r != null) {
          return 1;
        }
        return 0;
      }
    });

    Map<K, Pair<T, U>> state = new HashMap<>();
    final LeftJoinPairMonoid<T, U> m = new LeftJoinPairMonoid<>();
    List<Pair<Long, Pair<K, Pair<T, U>>>> res = new ArrayList<>();
    for (Pair<Long, Pair<K, Pair<T, U>>> p : l3) {
      K k = p.r.l;

      Pair<T, U> s = state.get(k);
      s = m.plus(s, p.r.r);
      state.put(k, s);
      if (s.l != null) {
        res.add(new Pair<>(p.l, new Pair<>(k, s)));
      }
    }
    return new com.airwallex.airskiff.testhelpers.KList<>(res, p -> p.r.l);
  }

  private <K, T, U, W extends Window> KList<K, Pair<Long, Pair<K, U>>> compileWindow(
    WindowedStream<K, T, U, W> stream
  ) {
    KList<K, Pair<Long, Pair<K, T>>> ks = compileKS(stream.stream);
    Window w = stream.window;
    NamedSerializableIterableLambda<T, U> f = stream.f;
    Comparator<T> comparator = stream.comparator;
    List<Pair<Long, Pair<K, U>>> result = new ArrayList<>(ks.size());

    if (w instanceof EventTimeBasedSlidingWindow) {
      EventTimeBasedSlidingWindow sw = (EventTimeBasedSlidingWindow) w;
      ks.sort((o1, o2) -> new CompareToBuilder().append(o1.l, o2.l).append(o1.r.r, o2.r.r, comparator).toComparison());
      Map<K, SlidingWindowList<T>> windows = new HashMap<>();
      for (Pair<Long, Pair<K, T>> t : ks) {
        K key = t.r.l;
        SlidingWindowList<T> swl = windows.getOrDefault(key, new SlidingWindowList<>());
        windows.put(key, swl);
        Instant time = Instant.ofEpochMilli(t.l);
        swl.add(t.r.r, time);
        swl.moveStartTo(time.minus(sw.size()));
        Iterator<U> it = f.apply(swl.iter()).iterator();
        U last = null;
        while (it.hasNext()) {
          last = it.next();
        }
        if (last != null) {
          result.add(new Pair<>(t.l, new Pair<>(t.r.l, last)));
        }
      }
      return new KList<>(result, t -> t.r.l);
    }
    throw new IllegalArgumentException("window type not supported: " + w.getClass().getName());
  }

  private <T, U> List<Pair<Long, U>> compileMap(MapStream<T, U> ms) {
    var l = compile(ms.stream);
    return l.stream().map(t -> new Pair<>(t.l, ms.f.apply(t.r))).collect(Collectors.toList());
  }

  private <T> List<Pair<Long, T>> compileFilter(FilterStream<T> fs) {
    var l = compile(fs.stream);
    return l.stream().filter(t -> fs.p.apply(t.r)).collect(Collectors.toList());
  }

  private <T> List<Pair<Long, T>> compileConcat(ConcatStream<T> stream) {
    var l1 = compile(stream.a);
    var l2 = compile(stream.b);
    List<Pair<Long, T>> l3 = new ArrayList<>(l1);
    l3.addAll(l2);
    return l3;
  }

  private <T, U> List<Pair<Long, U>> compileFlat(FlatMapStream<T, U> fms) {
    var l = compile(fms.stream);

    var results = new ArrayList<Pair<Long, U>>();

    l.forEach(t -> {
      fms.f.apply(t.r).forEach(o -> results.add(new Pair<>(t.l, o)));
    });

    return results;
  }

  private <K, T> List<Pair<Long, Pair<K, T>>> sum(KList<K, Pair<Long, Pair<K, T>>> l, NamedMonoid<T> m) {
    var results = new ArrayList<Pair<Long, Pair<K, T>>>();
    var partial = new HashMap<K, T>();

    for (Pair<Long, Pair<K, T>> p : l) {
      K key = p.r.l;
      T previous = partial.getOrDefault(key, m.zero());
      T cur = m.plus(previous, p.r.r);
      partial.put(key, cur);
      results.add(new Pair<>(p.l, new Pair<>(key, cur)));
    }

    return results;
  }

  private <K, T> KList<K, Pair<Long, Pair<K, T>>> compileSum(SummedStream<K, T> ss) {
    var ll = compileKS(ss.stream);
    return new KList<>(sum(ll, ss.monoid), x -> x.r.l);
  }

  private <K, T> KList<K, Pair<Long, Pair<K, T>>> compileOrderedSum(OrderedSummedStream<K, T> oss) {
    var ll = compileKS(oss.stream);
    ll.sort((o1, o2) -> new CompareToBuilder().append(o1.l, o2.l).append(o1.r.r, o2.r.r, oss.order).toComparison());

    return new KList<>(sum(ll, oss.monoid), x -> x.r.l);
  }

  private <K, T> KList<K, Pair<Long, Pair<K, T>>> compileKS(KStream<K, T> ks) {
    if (ks instanceof KeyedSimpleStream) {
      KeyedSimpleStream<K, T> s = (KeyedSimpleStream<K, T>) ks;
      List<Pair<Long, T>> l = compile(s.stream);
      List<Pair<Long, Pair<K, T>>> kl = new ArrayList<>();
      for (Pair<Long, T> p : l) {
        kl.add(new Pair<>(p.l, new Pair<>(s.toKey.apply(p.r), p.r)));
      }
      return new KList<>(kl, x -> x.r.l);
    }
    if (ks instanceof LeftJoinStream) {
      return (KList<K, Pair<Long, Pair<K, T>>>) (List) compileLeftJoin((LeftJoinStream<K, ?, ?>) ks);
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
      return compileWindow((WindowedStream<K, ?, T, ?>) ks);
    }
    throw new IllegalArgumentException("Unknown KStream type: " + ks.getClass());
  }

  private <K, T, U> KList<K, Pair<Long, Pair<K, U>>> mapValue(MapValueStream<K, T, U> ks) {
    KList<K, Pair<Long, Pair<K, T>>> ll = compileKS(ks.stream);
    List<Pair<Long, Pair<K, U>>> res = new ArrayList<>();
    for (Pair<Long, Pair<K, T>> p : ll) {
      res.add(new Pair<>(p.l, new Pair<>(p.r.l, ks.fn.apply(p.r.r))));
    }
    return new KList<>(res, x -> x.r.l);
  }

  private <T, U> List<Pair<Long, U>> compileSql(SqlStream<T, U> ss) {
    Class<T> tc = StreamUtils.clz(ss.stream);
    Field[] fds = StreamUtils.getFields(tc);

    String tableName = ss.tableName;

    var list = compile(ss.stream);

    try (Connection conn = DriverManager.getConnection(h2Url)) {
      Statement statement = conn.createStatement();
      String sql = "DROP TABLE IF EXISTS " + tableName + "; ";
      statement.execute(sql);

      sql = buildCreateTableSQL(tableName, fds);
      statement.execute(sql);
      // Insert rows
      for (Pair<Long, T> item : list) {
        sql = buildInsertRowSQL(tableName, item.l, item.r, fds);
        statement.execute(sql);
      }
      // Query rows
      String select = ss.sql.substring(0, 6);
      sql = ss.sql.replaceFirst(select, select + " ts__, ").replaceAll("DAY\\(\\d+\\)", "DAY");

      ResultSet result = statement.executeQuery(sql);
      return castQueryResultSetToList(result, ss.mapper);
    } catch (Exception e) {
      // TODO
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }

  private String buildCreateTableSQL(String tableName, Field[] fds) {
    StringBuilder createTableSqlBuilder = new StringBuilder("CREATE TABLE ").append(tableName).append("(ts__ BIGINT, ");
    for (Field fd : fds) {
      String sqlDataType = mappingToSqlDataType(fd);
      createTableSqlBuilder.append(fd.getName()).append(" ").append(sqlDataType).append(", ");
    }
    createTableSqlBuilder.append("row_time__ TIMESTAMP").append(");");
    return createTableSqlBuilder.toString();
  }

  private <T> String buildInsertRowSQL(String tableName, Long ts, T row, Field[] fds) throws Exception {
    StringBuilder insertRowSqlBuilder = new StringBuilder("INSERT INTO " + tableName + " VALUES (" + ts + ",");
    for (Field fd : fds) {
      Field f = row.getClass().getField(fd.getName());
      if (mappingToSqlDataType(f).equals("VARCHAR(250)") && f.get(row) != null) {
        insertRowSqlBuilder.append("'").append(f.get(row)).append("'");
      } else {
        insertRowSqlBuilder.append(f.get(row));
      }
      insertRowSqlBuilder.append(",");
    }
    insertRowSqlBuilder.append("'")
      .append(Instant.ofEpochMilli(ts).atZone(ZoneOffset.UTC).toLocalDateTime().format(formatter))
      .append("'");
    insertRowSqlBuilder.append(")");
    return insertRowSqlBuilder.toString();
  }

  private String mappingToSqlDataType(Field fd) {
    // Because of this compiler is only for testing, supports Number, String is enough.
    String[] a = fd.getType().getTypeName().split("\\.");
    String t = a[a.length - 1];
    switch (t) {
      case "String":
        return "VARCHAR(250)";
      case "Integer":
      case "int":
        return "INTEGER";
      case "Long":
      case "long":
        return "BIGINT";
      case "Float":
        return "REAL";
      case "Double":
      case "double":
        return "DOUBLE";
      case "boolean":
      case "Boolean":
        return "BOOLEAN";
      default:
        throw new IllegalArgumentException(t + " not supported for SQL testing");
    }
  }

  private <U> List<Pair<Long, U>> castQueryResultSetToList(
    ResultSet result, MapFunction<Row, U> mapper
  ) throws SQLException {
    int columnCount = result.getMetaData().getColumnCount();

    List<Row> rows = new ArrayList<>(result.getFetchSize());
    while (result.next()) {
      Row row = new Row(columnCount);
      for (int i = 0; i < columnCount; i++) {
        // result columns start with 1 indexing for some awful reason
        row.setField(i, result.getObject(i + 1));
      }
      rows.add(row);
    }

    List<Pair<Long, U>> list = new ArrayList<>();
    for (Row r : rows) {
      try {
        Long ts = (Long) r.getField(0);
        U u = mapper.map(r);
        list.add(new Pair(ts, u));
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return list;
  }
}
