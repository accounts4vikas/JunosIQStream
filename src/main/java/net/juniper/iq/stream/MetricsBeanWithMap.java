package net.juniper.iq.stream;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class MetricsBeanWithMap implements Serializable {
	String key;
	Date time;
	Map<String, BigInteger> aggValues;
	
	public MetricsBeanWithMap() { }

	public MetricsBeanWithMap(String key, Date time,
			Map<String, BigInteger> aggValues) {
		this.key = key;
		this.time = time;
		this.aggValues = aggValues;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public Map<String, BigInteger> getAggValues() {
		return aggValues;
	}

	public void setAggValues(Map<String, BigInteger> aggValues) {
		this.aggValues = aggValues;
	}

	@Override
	public String toString() {
		StringBuffer aggValuesStr = new StringBuffer();
		aggValuesStr.append(" [min= " + aggValues.get("min"));
		aggValuesStr.append(", max= " + aggValues.get("max"));
		aggValuesStr.append(", avg= " + aggValues.get("avg") + "]");
		
		return "MetricsBeanWithMap [key=" + key + ", time=" + time
				+ ", aggValues=" + aggValuesStr.toString() + "]";
	}
	
	
}
