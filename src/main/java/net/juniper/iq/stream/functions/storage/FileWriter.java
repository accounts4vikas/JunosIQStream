package net.juniper.iq.stream.functions.storage;

import java.math.BigInteger;

import net.juniper.iq.stream.Properties;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;


public class FileWriter
    implements Function2<JavaPairRDD<String,BigInteger>,
                         Time,
                         Void>
{
    private static final long serialVersionUID = 42l;

    //@Override
    public Void call(
    		JavaPairRDD<String,BigInteger> rdd,
        Time time)
    {
        if (rdd.count() <= 0) return null;
        String path = Properties.getString("stream.output_file") +
                      "_" +
                      time.milliseconds();
        rdd.saveAsTextFile(path);
        return null;
    }
}
