# JunosIQ Streaming Pipeline

This is a project for demonstrating and testing Kafka and Spark Streaming 
using JVision log messages. This project consume cpu_mem json messages
from Kafka using Spark Streaming and then create minimum, maximum & 
average for HeapInfo for sources supplied in input data (e.g. Kernel, 
LAN buffer etc.). For each Spark Stream batch duration, calculated results 
are finally inserted into KeyStats table in mysql database., which can be
used to visualize that data through some UI application.  


# Setting up locally on your machine

Clone the project and download on your machine
```
git clone git@github.com:accounts4vikas/JunosIQStream.git
```

Compile the code using maven:

```
% mvn package
```

Create KeyStats table in your mysql database use following:

```
CREATE TABLE `KeyStats` (
  `PROCESSED_AT` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `KEY_NAME` varchar(30) NOT NULL,
  `MINIMUM_VAL` int(11) NOT NULL,
  `MAXIMUM_VAL` int(11) NOT NULL,
  `AVERAGE_VAL` int(11) NOT NULL,
  PRIMARY KEY (`PROCESSED_AT`,`KEY_NAME`)
);
 ```

To configure MYSQL DB details, following properties need to be set

```
stream.mysql.url: ${YOUR_MYSQL_URL}
stream.mysql.userid: ${YOUR_MYSQL_USER}
```

# Running the demo

To run StreamingDemo, you can use spark-submit program or use supplied runDemo.sh script

```
%  ${YOUR_SPARK_HOME}/bin/spark-submit
   --files {YOURS_log4j.properties_FILE_PATH} \
   --class "net.juniper.iq.stream.StreamingDemo"
   --master local[4]
   ./target/JunosIQStream-0.0.1-SNAPSHOT.jar \
   $@ 
```

Kafka and Spark Streaming properties needs to configured before running StreamingDemo

```
stream.zkhosts: ${YOUR_ZOOKEPPER_HOST}
stream.kafka_topic: ${YOUR_KAFKA_TOPIC}
stream.sparkstream.batch_interval: ${YOUR_SPARK_STREAMING_BATCH_INTERVAL_IN_SECS}

```
 
Note: You can use KafkaJsonProducer to create a stream of Test Json messages for testing purpose.
