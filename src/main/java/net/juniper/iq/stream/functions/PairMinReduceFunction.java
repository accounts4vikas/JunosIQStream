package net.juniper.iq.stream.functions;


import java.math.BigInteger;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class PairMinReduceFunction implements
		Function2<BigInteger,BigInteger, BigInteger> {
	private static final long serialVersionUID = 42l;

	@Override
	public BigInteger call(BigInteger input1, BigInteger input2) {
		if(input1.intValue() < input2.intValue()) {
			return input1;
		} else {
			return input2;
		}
	}

}
