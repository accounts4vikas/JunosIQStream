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

@JsonPropertyOrder({ "slot", "sensor_id", "pfe", "name", "length", "version",
		"fid" })
public class Header {

	@JsonProperty("slot")
	private long slot;
	@JsonProperty("sensor_id")
	private long sensorId;
	@JsonProperty("pfe")
	private long pfe;
	@JsonProperty("name")
	private long name;
	@JsonProperty("length")
	private long length;
	@JsonProperty("version")
	private long version;
	@JsonProperty("fid")
	private long fid;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The slot
	 */
	@JsonProperty("slot")
	public long getSlot() {
		return slot;
	}

	/**
	 * 
	 * @param slot
	 *            The slot
	 */
	@JsonProperty("slot")
	public void setSlot(long slot) {
		this.slot = slot;
	}

	public Header withSlot(long slot) {
		this.slot = slot;
		return this;
	}

	/**
	 * 
	 * @return The sensorId
	 */
	@JsonProperty("sensor_id")
	public long getSensorId() {
		return sensorId;
	}

	/**
	 * 
	 * @param sensorId
	 *            The sensor_id
	 */
	@JsonProperty("sensor_id")
	public void setSensorId(long sensorId) {
		this.sensorId = sensorId;
	}

	public Header withSensorId(long sensorId) {
		this.sensorId = sensorId;
		return this;
	}

	/**
	 * 
	 * @return The pfe
	 */
	@JsonProperty("pfe")
	public long getPfe() {
		return pfe;
	}

	/**
	 * 
	 * @param pfe
	 *            The pfe
	 */
	@JsonProperty("pfe")
	public void setPfe(long pfe) {
		this.pfe = pfe;
	}

	public Header withPfe(long pfe) {
		this.pfe = pfe;
		return this;
	}

	/**
	 * 
	 * @return The name
	 */
	@JsonProperty("name")
	public long getName() {
		return name;
	}

	/**
	 * 
	 * @param name
	 *            The name
	 */
	@JsonProperty("name")
	public void setName(long name) {
		this.name = name;
	}

	public Header withName(long name) {
		this.name = name;
		return this;
	}

	/**
	 * 
	 * @return The length
	 */
	@JsonProperty("length")
	public long getLength() {
		return length;
	}

	/**
	 * 
	 * @param length
	 *            The length
	 */
	@JsonProperty("length")
	public void setLength(long length) {
		this.length = length;
	}

	public Header withLength(long length) {
		this.length = length;
		return this;
	}

	/**
	 * 
	 * @return The version
	 */
	@JsonProperty("version")
	public long getVersion() {
		return version;
	}

	/**
	 * 
	 * @param version
	 *            The version
	 */
	@JsonProperty("version")
	public void setVersion(long version) {
		this.version = version;
	}

	public Header withVersion(long version) {
		this.version = version;
		return this;
	}

	/**
	 * 
	 * @return The fid
	 */
	@JsonProperty("fid")
	public long getFid() {
		return fid;
	}

	/**
	 * 
	 * @param fid
	 *            The fid
	 */
	@JsonProperty("fid")
	public void setFid(long fid) {
		this.fid = fid;
	}

	public Header withFid(long fid) {
		this.fid = fid;
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

	public Header withAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
		return this;
	}

}