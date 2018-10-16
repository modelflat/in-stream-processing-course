# Fraud detection capstone project

```
Producer    ->    FS     ->      Kafka    ->   Spark Streaming          ->           Cassandra
         (fwrite)    (connector)       (spark)        |        (streaming-cassandra)
                                                      |
                                              Ignite (store state)
```