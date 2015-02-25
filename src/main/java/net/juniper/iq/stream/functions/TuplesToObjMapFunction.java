package net.juniper.iq.stream.functions;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import net.juniper.iq.stream.MetricsBeanWithMap;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class TuplesToObjMapFunction implements Function<Tuple2<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>>, MetricsBeanWithMap> {
	private static final long serialVersionUID = 42l;

	@Override
	public MetricsBeanWithMap call(
			Tuple2<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>> input) {
		MetricsBeanWithMap metricsBean = new MetricsBeanWithMap();
		metricsBean.setKey(input._1);
		metricsBean.setTime(new java.util.Date(System.currentTimeMillis()));
		Map<String, BigInteger> aggValues = new HashMap<String, BigInteger>();
		aggValues.put(FunctionConstants.MIN, input._2._1._1);
		aggValues.put(FunctionConstants.MAX, input._2._1._2);
		aggValues.put(FunctionConstants.AVG, input._2._2);
		metricsBean.setAggValues(aggValues);
		return metricsBean;
	}
}
