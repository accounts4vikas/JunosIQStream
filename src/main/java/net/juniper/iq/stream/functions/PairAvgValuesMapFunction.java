package net.juniper.iq.stream.functions;

import java.math.BigInteger;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class PairAvgValuesMapFunction implements Function<Tuple2<BigInteger,BigInteger>,BigInteger> {
	private static final long serialVersionUID = 42l;

	@Override
	public BigInteger call(Tuple2<BigInteger,BigInteger> input) {
		return input._1.divide(input._2);
	}
}
