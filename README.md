# AirSkiff

A stream library for data processing in batch and real-time. It has the
capability to run the same stream definition on top of different
data processing frameworks, such as Flink, Spark, etc.

More details can be found [here](https://medium.com/airwallex-engineering/lessons-from-building-a-feature-store-on-flink-4604d8fb9c80)
about how we use it at Airwallex to build our feature store.

## Features
* Cross-platform capability to run the same `Stream` on different frameworks,
such as Flink, Spark, etc.
* Inherited features from underlying frameworks, such as exactly-once
  processing guarantees and flexible windowing from Flink
* One `Stream` definition that can run in batch mode or real-time streaming mode
* SQL and functional transformations are integrated seamlessly
* A built-in concept of event time to avoid time traveling
* Convenient and intuitive method calls such as `mapValue`, `leftJoin`,
  and `previousAndCurrent` for keyed `Stream`
* In addition, `leftJoin` is point-in-time join by default

## Installation
The library is available in Maven central repository.
### Maven
```maven
<dependency>
    <groupId>com.airwallex.airskiff</groupId>
    <artifactId>core</artifactId>
    <version>0.0.6</version>
</dependency>
```
### Gradle (Kotlin)
```gradle
implementation("com.airwallex.airskiff:core:0.0.6")
```

## Example
For a word count program, the main body will look like:
```Java
FlinkLocalTextConfig config = new FlinkLocalTextConfig("/path/to/file");
Stream<Counter> stream = new SourceStream<>(config)
  .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
  .map(x -> new Counter(x, 1L), Counter.class)
  .keyBy(x -> x.key, String.class)
  .sum((a, b) -> new Counter(b.key, a == null ? 0 : a.c + b.c))
  .values();

// for batch processing
new FlinkBatchCompiler(env, tableEnv).compile(stream).print();

// for real time processing
// new FlinkRealtimeCompiler(env, tableEnv).compile(stream).print();
env.execute();
```

We can also implement the above `Stream` using SQL:
```Java
public class Counter {
  public String key;
  public Long c;

  public Counter() {}

  public Counter(String key, Long c) {
    this.key = key;
    this.c = c;
  }
}

Stream<Counter> stream = new SourceStream<>(config)
  .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
  .map(x -> new Counter(x, 1L), Counter.class)
  .sql("SELECT key, COUNT(*) OVER (PARTITION BY key ORDER BY row_time__ RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM text", "text", Counter.class);
```
There are certain limitations about the SQL keywords we can support.
For instance, since we attach a timestamp to each event internally,
aggregations can only be done through `PARTITION BY`,
not `GROUP BY`. Otherwise, it's not clear which timestamp to be used
for the grouped events.

Runnable examples can be found [here](https://github.com/airwallex/AirSkiff/tree/master/core/src/main/java/com/airwallex/airskiff/examples).
