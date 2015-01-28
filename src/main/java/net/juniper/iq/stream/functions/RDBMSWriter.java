package net.juniper.iq.stream.functions;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;

import net.juniper.iq.stream.Properties;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;

import scala.Tuple2;

public class RDBMSWriter
		implements
		Function2<JavaPairRDD<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>>, Time, Void> {
	private static final long serialVersionUID = 42l;

	public Void call(
			JavaPairRDD<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>> rdd,
			final Time time) throws Exception {
		if (rdd.count() <= 0)
			return null;

		rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>>>>() {
			private Connection dbConn = null;
			private PreparedStatement stmt = null;

			public void call(
					Iterator<Tuple2<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>>> itr)
					throws Exception {
				try {
					Class.forName("com.mysql.jdbc.Driver");
					String url = Properties.getString("stream.mysql.url");
					String userid = Properties.getString("stream.mysql.userid");
					dbConn = DriverManager.getConnection(url, userid, "");

					while (itr.hasNext()) {
						Tuple2<String, Tuple2<Tuple2<BigInteger, BigInteger>, BigInteger>> element = itr
								.next();
						String insertTableSQL = "INSERT INTO KEYSTATS "
								+ "(PROCESSED_AT, KEY_NAME, MINIMUM_VAL, MAXIMUM_VAL, AVERAGE_VAL) VALUES"
								+ "(?,?,?,?,?)";
						stmt = dbConn.prepareStatement(insertTableSQL);
						stmt.setTimestamp(1, new Timestamp(time.milliseconds()));
						stmt.setString(2, element._1);
						stmt.setInt(3, element._2._1._1.intValue());
						stmt.setInt(4, element._2._1._2.intValue());
						stmt.setInt(5, element._2._2.intValue());
						stmt.execute();
					}
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				} finally {
					dbConn.close();
				}
			}
		});
		return null;
	}

}
