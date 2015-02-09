package net.juniper.iq.stream;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import net.juniper.iq.stream.functions.HeapInfoFlatMapFunction;
import net.juniper.iq.stream.functions.RDBMSWriter;
import net.juniper.iq.stream.functions.UtilizationPairFunction;
import net.juniper.iq.stream.functions.UtilizationReduceFunction;
import net.juniper.iq.stream.jvision.HeapInfo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;


public class StreamingDemo {

	
	private static final String KAFKA_TOPIC = Properties
			.getString("stream.kafka_topic");
	private static final int KAFKA_PARALLELIZATION = Properties
			.getInt("stream.kafka_parallelization");
	private static final int SPARK_STREAM_BATCH_INTERVAL = Properties
			.getInt("stream.sparkstream.batch_interval");
	
	public static void main(String[] args) {		
		SparkConf conf = new SparkConf().setAppName("JunosIQStreamApp");
        
		if (args.length > 0)
            conf.setMaster(args[0]);
        else
            conf.setMaster("local[4]");
        
		JavaStreamingContext ssc = new JavaStreamingContext(conf,
				Durations.seconds(SPARK_STREAM_BATCH_INTERVAL));
		ssc.checkpoint("/tmp");
 
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(KAFKA_TOPIC, KAFKA_PARALLELIZATION);
		
		JavaPairReceiverInputDStream<String, String> kafkaMsgs = KafkaUtils
				.createStream(ssc, Properties.getString("stream.zkhosts"),
						"test-consumer-group", topicMap, StorageLevel.MEMORY_AND_DISK());

		JavaDStream<String> jsonDS = kafkaMsgs
				.map(new Function<Tuple2<String, String>, String>() {
					public String call(Tuple2<String, String> message) {
						return message._2();
					}
				});
		
		JavaDStream<HeapInfo> heapInfoFlatMapDS = jsonDS.flatMap(new HeapInfoFlatMapFunction());
		JavaPairDStream<String,BigInteger> kvMappedDS = heapInfoFlatMapDS.mapToPair(new UtilizationPairFunction()).cache();
		

		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvAvgMappedDS = kvMappedDS.mapValues(new Function<BigInteger,Tuple2<BigInteger,BigInteger>>() {
			public Tuple2<BigInteger,BigInteger> call(BigInteger input) {
				BigInteger bigInt = BigInteger.valueOf(1);
				return new Tuple2(input, bigInt);
			}			
		});
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvAvgReducedByKeyDS = kvAvgMappedDS.reduceByKey(new UtilizationReduceFunction());
		
		JavaPairDStream<String,BigInteger> kvAvgResultPairDS = kvAvgReducedByKeyDS.mapValues(new Function<Tuple2<BigInteger,BigInteger>,BigInteger>() {
			public BigInteger call(Tuple2<BigInteger,BigInteger> input) {
				return input._1.divide(input._2);
			}			
		});


		JavaPairDStream<String,BigInteger> kvMinReducedDS = kvMappedDS.reduceByKey(new Function2<BigInteger,BigInteger, BigInteger>() {
			public BigInteger call(BigInteger input1, BigInteger input2) {
				if(input1.intValue() < input2.intValue()) {
					return input1;
				} else {
					return input2;
				}
			}			
		});
		
		JavaPairDStream<String,BigInteger> kvMaxReducedDS = kvMappedDS.reduceByKey(new Function2<BigInteger,BigInteger, BigInteger>() {
			public BigInteger call(BigInteger input1, BigInteger input2) {
				if(input1.intValue() < input2.intValue()) {
					return input2;
				} else {
					return input1;
				}
			}			
		});
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvMinMaxJoinedDS = kvMinReducedDS.join(kvMaxReducedDS);
		
		JavaPairDStream<String,Tuple2<Tuple2<BigInteger,BigInteger>,BigInteger>> kvResultsDS = kvMinMaxJoinedDS.join(kvAvgResultPairDS);

		//kvResultsDS.foreachRDD(new RDBMSWriter());
		
		kvResultsDS.print();
		//kvResultsDS.foreachRDD(FileWriter);

		ssc.start();
		
		ssc.awaitTermination();		
	}

}
