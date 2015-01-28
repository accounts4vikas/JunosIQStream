package net.juniper.iq.stream.functions;


import java.math.BigInteger;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class UtilizationReduceFunction implements
		Function2<Tuple2<BigInteger,BigInteger>, Tuple2<BigInteger,BigInteger>, Tuple2<BigInteger,BigInteger>> {
	private static final long serialVersionUID = 42l;

	public Tuple2<BigInteger,BigInteger> call(Tuple2<BigInteger,BigInteger> x, Tuple2<BigInteger,BigInteger> y) throws Exception {
		return new Tuple2<BigInteger,BigInteger>(x._1.add(y._1), x._2.add(y._2));
	}

}
