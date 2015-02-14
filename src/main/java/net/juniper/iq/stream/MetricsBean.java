package net.juniper.iq.stream;

import java.io.Serializable;

public class MetricsBean implements Serializable {
	String key;
	String aggType;
	Long timestamp;
	Long value;
	
	public MetricsBean() { }

	public MetricsBean(String key, String aggType, Long timestamp, Long value) {
		this.key = key;
		this.aggType = aggType;
		this.timestamp = timestamp;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getAggType() {
		return aggType;
	}

	public void setAggType(String aggType) {
		this.aggType = aggType;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "MetricsBean [key=" + key + ", aggType=" + aggType
				+ ", timestamp=" + timestamp + ", value=" + value + "]";
	}
	
}
