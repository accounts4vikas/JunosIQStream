package net.juniper.iq.stream.functions.storage;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Iterator;

import net.juniper.iq.stream.MetricsBeanWithMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;

import scala.Function1;
import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class CassandraWriter implements
		Function2<JavaRDD<MetricsBeanWithMap>, Time, Void> {
	private static final long serialVersionUID = 42l;

	public Void call(JavaRDD<MetricsBeanWithMap> rdd, Time time)
			throws Exception {
		if (rdd.count() <= 0)
			return null;
		final SparkConf conf  = rdd.context().conf();
		
		rdd.foreachPartition(new VoidFunction<Iterator<MetricsBeanWithMap>>() {
			CassandraConnector connector = CassandraConnector.apply(conf);
	        Session session = connector.openSession();

			public void call(
					Iterator<MetricsBeanWithMap> itr)
					throws Exception {
				try {
					while (itr.hasNext()) {
						MetricsBeanWithMap element = itr.next();
						StringBuffer insertQuer = new StringBuffer("INSERT INTO JunosIQStreamDB.metrics_ts_withmap_data (key, time, agg_values) " +
							      "VALUES ('" + element.getKey() + "'," + element.getTime() 
							      + ", {'min' : " + element.getAggValues().get("min") 
							      + ",'max' : " + element.getAggValues().get("max") 
							      + ", 'avg' : " + element.getAggValues().get("avg") + "});");
						System.out.println(insertQuer.toString());	      
							      
				      //session.execute(insertQuer);

					}					
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					session.close();
				}
			}
		});
		
		return null;
	}
	

}
