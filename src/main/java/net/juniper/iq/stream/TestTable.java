package net.juniper.iq.stream;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class TestTable implements Serializable {
	String key;
	Date time;
	BigInteger value;
	
	public TestTable() { }

	public TestTable(String key, Date time, BigInteger value) {
		this.key = key;
		this.time = time;
		this.value = value;
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

	public BigInteger getValue() {
		return value;
	}

	public void setValue(BigInteger value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "TestTable [key=" + key + ", time=" + time
				+ ", value=" + value + "]";
	}


}
