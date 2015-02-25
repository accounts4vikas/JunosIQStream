package net.juniper.iq.stream;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import net.juniper.iq.stream.functions.KafkaMsgToJsonMapFunction;
import net.juniper.iq.stream.functions.ObjToKVPairFunction;
import net.juniper.iq.stream.functions.PairAvgValuesMapFunction;
import net.juniper.iq.stream.functions.PairCountReduceFunction;
import net.juniper.iq.stream.functions.PairCountValueMapFunction;
import net.juniper.iq.stream.functions.PairMaxReduceFunction;
import net.juniper.iq.stream.functions.PairMinReduceFunction;
import net.juniper.iq.stream.functions.ToJsonObjFlatMapFunction;
import net.juniper.iq.stream.functions.TuplesToObjMapFunction;
import net.juniper.iq.stream.jvision.HeapInfo;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class StreamingDemo {

	private static final String KAFKA_TOPIC_KEY = "stream.kafka_topic";
	private static final String KAFKA_PARALLEL_KEY = "stream.kafka_parallelization";
	private static final String SPARK_ZOOKEEPER_KEY = "stream.zkhosts";

	private static final String SPARK_STREAM_BATCH_KEY = "stream.batch_interval";

	private static final String SPARK_CASS_URL_KEY = "stream.cassandra.url";
	private static final String SPARK_CASS_CONN_KEY = "spark.cassandra.connection.host";

	private static final String KAFKA_CONSUMER_GROUP_KEY = "stream.kafka_consumer_group_id";	
	private static final String SPARK_CP_DIR_KEY = "stream.checkpoint.dir";	
	private static final String CASS_KEYSPACE_KEY = "stream.cassandra.keyspace";	
	
	private static final String SPARK_APP_NAME_KEY = "stream.appname";	
	private static final String SPARK_SERVER_KEY = "stream.spark.server";
	
	private static final String CASS_SEC_CF_KEY = "stream.cassandra.table";

	private static final String CASS_CF_INTV1_KEY = "stream.cassandra.table.interval1";
	private static final String CASS_CF_INTV2_KEY = "stream.cassandra.table.interval2";
	private static final String CASS_CF_INTV3_KEY = "stream.cassandra.table.interval3";
	
	private static final String SPARK_WINDOW_SIZE1_KEY = "stream.window.size.interval1";
	private static final String SPARK_WINDOW_SIZE2_KEY = "stream.window.size.interval2";
	private static final String SPARK_WINDOW_SIZE3_KEY = "stream.window.size.interval3";

    private transient SparkConf conf;

    private StreamingDemo(SparkConf conf) {
        this.conf = conf;
    }
	
    private void run() {
		JavaStreamingContext ssc = new JavaStreamingContext(conf,
				Durations.seconds(Properties
						.getInt(SPARK_STREAM_BATCH_KEY)));
		
		ssc.checkpoint(Properties
				.getString(SPARK_CP_DIR_KEY));
		
		//compute(ssc);
        
		computeWindows(ssc);

		ssc.awaitTermination();
    } 
    
    private void computeWindows(JavaStreamingContext ssc) {
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(Properties
				.getString(KAFKA_TOPIC_KEY), Properties
				.getInt(KAFKA_PARALLEL_KEY));
		
		JavaPairReceiverInputDStream<String, String> kafkaMsgs = KafkaUtils
				.createStream(ssc, Properties.getString(SPARK_ZOOKEEPER_KEY),
						Properties.getString(KAFKA_CONSUMER_GROUP_KEY),
						topicMap, StorageLevel.MEMORY_AND_DISK());

		JavaDStream<String> jsonDS = kafkaMsgs.map(new KafkaMsgToJsonMapFunction());

		JavaDStream<HeapInfo> heapInfoFlatMapDS = jsonDS.flatMap(new ToJsonObjFlatMapFunction()).cache();
		
		JavaDStream<HeapInfo> interval1WindowDS = heapInfoFlatMapDS.window(
				new Duration(Properties.getInt(SPARK_WINDOW_SIZE1_KEY)),
				new Duration(Properties.getInt(SPARK_WINDOW_SIZE1_KEY)));
		JavaDStream<HeapInfo> interval2WindowDS = heapInfoFlatMapDS.window(
				new Duration(Properties.getInt(SPARK_WINDOW_SIZE2_KEY)),
				new Duration(Properties.getInt(SPARK_WINDOW_SIZE2_KEY)));
		JavaDStream<HeapInfo> interval3WindowDS = heapInfoFlatMapDS.window(
				new Duration(Properties.getInt(SPARK_WINDOW_SIZE3_KEY)),
				new Duration(Properties.getInt(SPARK_WINDOW_SIZE3_KEY)));
		
		processWindow(interval1WindowDS,Properties.getString(CASS_CF_INTV1_KEY));
		processWindow(interval2WindowDS,Properties.getString(CASS_CF_INTV2_KEY));
		processWindow(interval3WindowDS,Properties.getString(CASS_CF_INTV3_KEY));
		
		//oneSecWindowDS.print();
		
		ssc.start();
    }   
    
    private void processWindow(JavaDStream<HeapInfo> windowDS, String tableName) {
		JavaPairDStream<String,BigInteger> kvMappedDS = windowDS.mapToPair(new ObjToKVPairFunction()).cache();
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvCountValueMappedDS = kvMappedDS.mapValues(new PairCountValueMapFunction());
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvCountReducedByKeyDS = kvCountValueMappedDS.reduceByKey(new PairCountReduceFunction());
		
		JavaPairDStream<String,BigInteger> kvAvgResultPairDS = kvCountReducedByKeyDS.mapValues(new PairAvgValuesMapFunction());

		JavaPairDStream<String,BigInteger> kvMinReducedDS = kvMappedDS.reduceByKey(new PairMinReduceFunction());
		
		JavaPairDStream<String,BigInteger> kvMaxReducedDS = kvMappedDS.reduceByKey(new PairMaxReduceFunction());
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvMinMaxJoinedDS = kvMinReducedDS.join(kvMaxReducedDS);
		
		JavaPairDStream<String,Tuple2<Tuple2<BigInteger,BigInteger>,BigInteger>> kvResultsDS = kvMinMaxJoinedDS.join(kvAvgResultPairDS);

		JavaDStream<MetricsBeanWithMap> kvResultsMappedDS = kvResultsDS.map(new TuplesToObjMapFunction());			
		
		//kvResultsMappedDS.print();
		
		javaFunctions(kvResultsMappedDS).writerBuilder(
				Properties.getString(CASS_KEYSPACE_KEY), tableName,
				mapToRow(MetricsBeanWithMap.class)).saveToCassandra();
    }     

    
    private void compute(JavaStreamingContext ssc) {
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(Properties
				.getString(KAFKA_TOPIC_KEY), Properties
				.getInt(KAFKA_PARALLEL_KEY));
		
		JavaPairReceiverInputDStream<String, String> kafkaMsgs = KafkaUtils
				.createStream(ssc, Properties.getString(SPARK_ZOOKEEPER_KEY),
						Properties.getString(KAFKA_CONSUMER_GROUP_KEY),
						topicMap, StorageLevel.MEMORY_AND_DISK());

		JavaDStream<String> jsonDS = kafkaMsgs.map(new KafkaMsgToJsonMapFunction());

		JavaDStream<HeapInfo> heapInfoFlatMapDS = jsonDS.flatMap(new ToJsonObjFlatMapFunction());
		
		JavaPairDStream<String,BigInteger> kvMappedDS = heapInfoFlatMapDS.mapToPair(new ObjToKVPairFunction()).cache();
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvCountValueMappedDS = kvMappedDS.mapValues(new PairCountValueMapFunction());
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvCountReducedByKeyDS = kvCountValueMappedDS.reduceByKey(new PairCountReduceFunction());
		
		JavaPairDStream<String,BigInteger> kvAvgResultPairDS = kvCountReducedByKeyDS.mapValues(new PairAvgValuesMapFunction());

		JavaPairDStream<String,BigInteger> kvMinReducedDS = kvMappedDS.reduceByKey(new PairMinReduceFunction());
		
		JavaPairDStream<String,BigInteger> kvMaxReducedDS = kvMappedDS.reduceByKey(new PairMaxReduceFunction());
		
		JavaPairDStream<String,Tuple2<BigInteger,BigInteger>> kvMinMaxJoinedDS = kvMinReducedDS.join(kvMaxReducedDS);
		
		JavaPairDStream<String,Tuple2<Tuple2<BigInteger,BigInteger>,BigInteger>> kvResultsDS = kvMinMaxJoinedDS.join(kvAvgResultPairDS);

		JavaDStream<MetricsBeanWithMap> kvResultsMappedDS = kvResultsDS.map(new TuplesToObjMapFunction());			

		
		javaFunctions(kvResultsMappedDS).writerBuilder(
				Properties.getString(CASS_KEYSPACE_KEY),
				Properties.getString(CASS_SEC_CF_KEY),
				mapToRow(MetricsBeanWithMap.class)).saveToCassandra();

		kvResultsMappedDS.print();
		//kvResultsDS.foreachRDD(new RDBMSWriter());

		ssc.start();
    }      
    
	public static void main(String[] args) {	
		SparkConf conf = new SparkConf().setAppName(Properties.getString(SPARK_APP_NAME_KEY));
		conf.set(SPARK_CASS_CONN_KEY, Properties
				.getString(SPARK_CASS_URL_KEY));
		
		String master;
		if (args.length > 0) {
			master= args[0];
		} else {
			master = Properties.getString(SPARK_SERVER_KEY);
		}
		conf.setMaster(master);
		System.out.println("***********************************************************");
		System.out.println("Using Spark Master = " + master);
		System.out.println("***********************************************************");
		
		StreamingDemo app = new StreamingDemo(conf);
        app.run();		
	}
	
}
