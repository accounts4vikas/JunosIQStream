package net.juniper.iq.stream.jvision;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)

@JsonPropertyOrder({ "utctime", "ms", "localtime" })
public class Metadata {

	@JsonProperty("utctime")
	private String utctime;
	@JsonProperty("ms")
	private long ms;
	@JsonProperty("localtime")
	private String localtime;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The utctime
	 */
	@JsonProperty("utctime")
	public String getUtctime() {
		return utctime;
	}

	/**
	 * 
	 * @param utctime
	 *            The utctime
	 */
	@JsonProperty("utctime")
	public void setUtctime(String utctime) {
		this.utctime = utctime;
	}

	public Metadata withUtctime(String utctime) {
		this.utctime = utctime;
		return this;
	}

	/**
	 * 
	 * @return The ms
	 */
	@JsonProperty("ms")
	public long getMs() {
		return ms;
	}

	/**
	 * 
	 * @param ms
	 *            The ms
	 */
	@JsonProperty("ms")
	public void setMs(long ms) {
		this.ms = ms;
	}

	public Metadata withMs(long ms) {
		this.ms = ms;
		return this;
	}

	/**
	 * 
	 * @return The localtime
	 */
	@JsonProperty("localtime")
	public String getLocaltime() {
		return localtime;
	}

	/**
	 * 
	 * @param localtime
	 *            The localtime
	 */
	@JsonProperty("localtime")
	public void setLocaltime(String localtime) {
		this.localtime = localtime;
	}

	public Metadata withLocaltime(String localtime) {
		this.localtime = localtime;
		return this;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

	public Metadata withAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
		return this;
	}

}