package net.juniper.iq.stream.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class KafkaMsgToJsonMapFunction implements Function<Tuple2<String, String>, String> {
	private static final long serialVersionUID = 42l;

	@Override
	public String call(Tuple2<String, String> message) throws Exception {
		return message._2();
	}
}
