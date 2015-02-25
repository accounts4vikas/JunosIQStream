package net.juniper.iq.stream.functions;

import java.math.BigInteger;

import net.juniper.iq.stream.jvision.HeapInfo;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ObjToKVPairFunction implements
		PairFunction<HeapInfo, String, BigInteger> {
	private static final long serialVersionUID = 42l;

	public Tuple2<String, BigInteger> call(HeapInfo heapInfo) throws Exception {
		return new Tuple2<String, BigInteger>(heapInfo.getName(),
				BigInteger.valueOf(heapInfo.getUtilization()));
	}
}
