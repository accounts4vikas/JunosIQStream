package net.juniper.iq.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.juniper.iq.stream.MetricsBean;
import net.juniper.iq.stream.MetricsBeanWithMap;
import net.juniper.iq.stream.Properties;
import net.juniper.iq.stream.functions.FunctionConstants;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class BatchDemo implements Serializable {
	private static final String CASS_KEYSPACE_KEY = "stream.cassandra.keyspace";
	private static final String CASS_CF_INTV3_KEY = "stream.cassandra.table.interval3";
	private static final String CASS_CF_INTV4_KEY = "stream.cassandra.table.interval4";
	
	private static final String SPARK_BATCH_APP_NAME_KEY = "batch.appname";
	private static final String SPARK_SERVER_KEY = "stream.spark.server";

	private static final String SPARK_CASS_URL_KEY = "stream.cassandra.url";
	private static final String SPARK_CASS_CONN_KEY = "spark.cassandra.connection.host";	

	private static final String INMEM_SPARKSQL_TABLE_METRICS = "metrics";	

	
    private transient SparkConf conf;

    private BatchDemo(SparkConf conf) {
        this.conf = conf;
    }

    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        compute(sc);
        sc.stop();
    }

    private void compute(JavaSparkContext sc) {    	
		CassandraJavaRDD<MetricsBeanWithMap> metricsRowsSmallIntRDD = javaFunctions(sc)
				.cassandraTable(Properties.getString(CASS_KEYSPACE_KEY),
						Properties.getString(CASS_CF_INTV3_KEY),
						mapRowTo(MetricsBeanWithMap.class));
		
		JavaRDD<MetricsBean> mappedSmallIntRDD = metricsRowsSmallIntRDD.map(new Function<MetricsBeanWithMap, MetricsBean>() {
		    public MetricsBean call(MetricsBeanWithMap beanwithMap) { 
		    	MetricsBean bean = new MetricsBean();
		    	bean.setKey(beanwithMap.getKey());
		    	bean.setTime(beanwithMap.getTime().getTime());
		    	bean.setMinValue(beanwithMap.getAggValues().get(FunctionConstants.MIN).longValue());
		    	bean.setMaxValue(beanwithMap.getAggValues().get(FunctionConstants.MAX).longValue());
		    	bean.setAvgValue(beanwithMap.getAggValues().get(FunctionConstants.AVG).longValue());
		    	return bean; 
		    }
		}).cache();
		
		CassandraJavaRDD<MetricsBeanWithMap> metricsRowsBigIntRDD = javaFunctions(sc)
				.cassandraTable(Properties.getString(CASS_KEYSPACE_KEY),
						Properties.getString(CASS_CF_INTV4_KEY),
						mapRowTo(MetricsBeanWithMap.class));
		
		JavaSQLContext sqlContextOuter = new JavaSQLContext(sc);
	    JavaSchemaRDD schemaRDD = sqlContextOuter.applySchema(mappedSmallIntRDD, MetricsBean.class);		
	    schemaRDD.registerTempTable(INMEM_SPARKSQL_TABLE_METRICS);
	    sqlContextOuter.sqlContext().cacheTable(INMEM_SPARKSQL_TABLE_METRICS);
	    
		if(metricsRowsBigIntRDD.count() == 0) {
			System.out.println("**********************************************");
			System.out.println("Inserting for first time");
			System.out.println("**********************************************");

			Row row = sqlContextOuter.sql("SELECT time FROM metrics ORDER BY time DESC LIMIT 1").collect().get(0);
		    final Date lastTime = new Date(row.getLong(0));

			JavaRDD<MetricsBeanWithMap> aggMBWithMapRDD = sqlContextOuter
					.sql("SELECT key, MIN(minValue), MAX(maxValue), SUM(avgValue), COUNT(*) FROM metrics GROUP BY key")
					.map(new Function<Row, MetricsBeanWithMap>() {
						public MetricsBeanWithMap call(Row row) {
							MetricsBeanWithMap bean = new MetricsBeanWithMap();
							bean.setKey(row.getString(0));
							bean.setTime(lastTime);
							Map<String, BigInteger> aggValues = new HashMap<String, BigInteger>();
							aggValues.put(FunctionConstants.MIN,
									BigInteger.valueOf(row.getLong(1)));
							aggValues.put(FunctionConstants.MAX,
									BigInteger.valueOf(row.getLong(2)));
							aggValues.put(
									FunctionConstants.AVG,
									BigInteger.valueOf(row.getLong(3)
											/ row.getLong(4)));
							bean.setAggValues(aggValues);
							return bean;
						}
					});
		    
			javaFunctions(aggMBWithMapRDD).writerBuilder(
					Properties.getString(CASS_KEYSPACE_KEY),
					Properties.getString(CASS_CF_INTV4_KEY),
					mapToRow(MetricsBeanWithMap.class)).saveToCassandra();		    
		    
		   	/*
		    JavaRDD<String> metricsRowsStrRDD = aggMBWithMapRDD.map(new Function<MetricsBeanWithMap, String>() {
                public String call(MetricsBeanWithMap cassandraRow) throws Exception {
                    return cassandraRow.toString();
                }
            });
		   	System.out.println("Data as MetricsBeanWithMap: \n" + StringUtils.join(metricsRowsStrRDD.toArray(), "\n"));	
		   	*/
			
		} else {
			System.out.println("**********************************************");
			System.out.println("Inserting for next 5 min ");
			System.out.println("**********************************************");

			org.joda.time.DateTime lastInsertTime = new org.joda.time.DateTime(metricsRowsBigIntRDD.first().getTime());
			org.joda.time.DateTime lastInsertTimePlus5 = lastInsertTime.plusMinutes(5);
			
			while(lastInsertTimePlus5.isBefore(org.joda.time.DateTime.now())) {
				StringBuffer sqlWhereSubStr = new StringBuffer();
				sqlWhereSubStr.append(" WHERE time > ");
				sqlWhereSubStr.append(lastInsertTime.getMillis());
				sqlWhereSubStr.append(" AND time <= ");
				sqlWhereSubStr.append(lastInsertTimePlus5.getMillis());
				String countCheckSQL = "SELECT COUNT(*) FROM metrics" + sqlWhereSubStr.toString();
				
				//String getFilteredDataSQL = "SELECT key, time, minValue, maxValue, avgValue FROM metrics" + countCheckSQLSubStr.toString();
				
				String getFilteredDataSQL = "SELECT key, MIN(minValue), MAX(maxValue), SUM(avgValue), COUNT(*) FROM metrics" + sqlWhereSubStr.toString() + " GROUP BY key";
				 
				
				Row row = sqlContextOuter.sql(countCheckSQL).collect().get(0);
			    long count = row.getLong((0));
				
			    if(count > 0) {
				    System.out.println("Found " + count + " records between " + lastInsertTime + " and " + lastInsertTimePlus5 + ". Processing...");
					
				    final Date lastTimeInner = new Date(lastInsertTimePlus5.getMillis());
				    JavaRDD<MetricsBeanWithMap> aggFilteredMBWithMapRDD = sqlContextOuter.sql(getFilteredDataSQL).map(new Function<Row, MetricsBeanWithMap>() {
					    public MetricsBeanWithMap call(Row row) { 
					    	MetricsBeanWithMap bean = new MetricsBeanWithMap();
					    	bean.setKey(row.getString(0));
					    	bean.setTime(lastTimeInner);
							Map<String, BigInteger> aggValues = new HashMap<String, BigInteger>();
							aggValues.put(FunctionConstants.MIN, BigInteger.valueOf(row.getLong(1)));
							aggValues.put(FunctionConstants.MAX, BigInteger.valueOf(row.getLong(2)));
							aggValues.put(FunctionConstants.AVG, BigInteger.valueOf(row.getLong(3)/row.getLong(4)));
							bean.setAggValues(aggValues);
					    	return bean; 
					    }
					});						
					
					javaFunctions(aggFilteredMBWithMapRDD).writerBuilder(
							Properties.getString(CASS_KEYSPACE_KEY),
							Properties.getString(CASS_CF_INTV4_KEY),
							mapToRow(MetricsBeanWithMap.class)).saveToCassandra();
				    System.out.println("Inserted new aggregate for records between " + lastInsertTime + " and " + lastInsertTimePlus5 + ".");

				    org.joda.time.DateTime tempTime = lastInsertTimePlus5;
			    	lastInsertTime = lastInsertTimePlus5;
			    	lastInsertTimePlus5 = tempTime.plusMinutes(5);
			    	
			    } else {
				    System.out.println("No entry found between " + lastInsertTime + " and " + lastInsertTimePlus5 + ". Moving to next 5 minutes...");
			    	org.joda.time.DateTime tempTime = lastInsertTimePlus5;
			    	lastInsertTime = lastInsertTimePlus5;
			    	lastInsertTimePlus5 = tempTime.plusMinutes(5);
			    }
			}
		    System.out.println("No more new records to be entered...");
		}

        /*
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        try (Session session = connector.openSession()) {
            session.execute("select * from junosiqstreamdb.metrics_agg_1min limit 1");
        }
        */
    }


    public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Properties.getString(SPARK_BATCH_APP_NAME_KEY));
		conf.set(SPARK_CASS_CONN_KEY, Properties
				.getString(SPARK_CASS_URL_KEY));
		
		String master;
		if (args.length > 0) {
			master= args[0];
		} else {
			master = Properties.getString(SPARK_SERVER_KEY);
		}
		conf.setMaster(master);        

        BatchDemo app = new BatchDemo(conf);
        app.run();
    }
}
