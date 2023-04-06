package com.airwallex.airskiff.core.api;

import com.airwallex.airskiff.common.functions.NamedSerializableLambda;
import com.airwallex.airskiff.core.*;

import java.io.Serializable;
import java.util.List;

public interface Stream<T> extends Serializable {
  /**
   * One to one mapping from T to U.
   */
  default <U> Stream<U> map(NamedSerializableLambda<T, U> f, Class<U> uc) {
    return new MapStream<>(this, f, uc);
  }

  /**
   * THIS IS AN EXPERIMENTAL API! CHANGES ARE EXPECTED! For this sql method only, there are several
   * restrictions of what T and U can be respectively. For T:
   *
   * <ul>
   *   <li>T must be public
   *   <li>All fields must be public
   *   <li>Must have a default constructor with no arguments
   * </ul>
   * <p>
   * For U:
   *
   * <ul>
   *   <li>All fields must be public
   *   <li>All fields must be defined in the same order as that of the fields in the query. The
   *       names don't matter, but the types must match.
   *   <li>The constructor of U must be public and also takes in the same list of fields in the
   *       select statement in the same order.
   * </ul>
   * <p>
   * For example, if we have "SELECT a, b FROM table1", and a is an Integer and b is a String then T
   * must be defined as
   *
   * <pre>
   *   public class T {
   *     public Integer a;
   *     public String b;
   *     public T() {}
   *     public T(Integer x, String y) {
   *       ...
   *     }
   *     ...
   *   }
   * </pre>
   * <p>
   * and U must be defined as:
   *
   * <pre>
   * public class U {
   *   public Integer different_name;
   *   public String some_other_name;
   *   public U(Integer x, String y) {
   *     ...
   *   }
   *   ...
   * }
   * </pre>
   * <p>
   * In addition, we have a few requirements about the query and the tableName:
   *
   * <ul>
   *   <li>The query and the tableName are case sensitive.
   *   <li>The tableName argument should match the table name used in the query. Or pre-registered
   *       somewhere else.
   *   <li>The query is retrieval only, and must start with `select` or `SELECT`.
   *   <li>Some SQL key words may not be supported. For example, `AS` is not supported. A work
   *       around is to split the query into two sql() method calls.
   * </ul>
   * <p>
   * An example method call can be
   *
   * <p>sql("SELECT a, b, c, ... from test_table", "test_table", MyData.class);
   */
  default <U> Stream<U> sql(String query, String tableName, Class<U> uc) {
    return new SqlStream<>(this, query, tableName, uc);
  }

  /**
   * Map each input event T to zero or more U's
   */
  default <U> Stream<U> flatMap(NamedSerializableLambda<T, Iterable<U>> f, Class<U> uc) {
    return new FlatMapStream<>(this, f, uc);
  }

  /**
   * Join another stream of T
   */
  default Stream<T> union(Stream<T> that) {
    return new ConcatStream<>(this, that);
  }

  /**
   * filter out events based on predicate p
   */
  default Stream<T> filter(NamedSerializableLambda<T, Boolean> p) {
    return new FilterStream<>(this, p);
  }

  /**
   * turn a Stream into a KStream using a toKey function
   */
  default <K> KStream<K, T> keyBy(NamedSerializableLambda<T, K> toKey, Class<K> kc) {
    return new KeyedSimpleStream<>(this, toKey, kc);
  }

  Class<T> getClazz();

  List<Stream> upstreams();
}
