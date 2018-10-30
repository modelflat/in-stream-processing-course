[![Build Status](https://travis-ci.org/modelflat/in-stream-processing-course.svg?branch=master)](https://travis-ci.org/modelflat/in-stream-processing-course)

# Fraud detection capstone project

```
Producer    ->    FS     ->      Kafka    ->   Spark Streaming          ->           Cassandra
         (fwrite)    (connector)       (spark)        |        (streaming-cassandra)
                                                      |
                                              Ignite (store state)
```