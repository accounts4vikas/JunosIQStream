package net.juniper.iq.stream;

import java.io.Serializable;

public class MetricsBean implements Serializable {
	String key;
	long time;
	long minValue;
	long maxValue;
	long avgValue;
	
	public MetricsBean() { }
	
	public MetricsBean(String key, long time, long min, long max, long avg) {

		this.key = key;
		this.time = time;
		this.minValue = min;
		this.maxValue = max;
		this.avgValue = avg;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getMinValue() {
		return minValue;
	}

	public void setMinValue(long minValue) {
		this.minValue = minValue;
	}

	public long getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(long maxValue) {
		this.maxValue = maxValue;
	}

	public long getAvgValue() {
		return avgValue;
	}

	public void setAvgValue(long avgValue) {
		this.avgValue = avgValue;
	}

	@Override
	public String toString() {
		return "MetricsBean [key=" + key + ", time=" + time + ", minValue="
				+ minValue + ", maxValue=" + maxValue + ", avgValue="
				+ avgValue + "]";
	}


	
}
