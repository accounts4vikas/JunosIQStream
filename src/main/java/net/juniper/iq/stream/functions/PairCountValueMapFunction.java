package net.juniper.iq.stream.functions;

import java.math.BigInteger;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class PairCountValueMapFunction implements Function<BigInteger,Tuple2<BigInteger,BigInteger>> {
	private static final long serialVersionUID = 42l;

	@Override
	public Tuple2<BigInteger,BigInteger> call(BigInteger input) {
		BigInteger bigInt = BigInteger.valueOf(1);
		return new Tuple2(input, bigInt);
	}
}
