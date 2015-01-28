package net.juniper.iq.stream.jvision;

import java.io.Serializable;
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

@JsonPropertyOrder({ "allocated", "utilization", "name", "size" })
public class HeapInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	
	@JsonProperty("allocated")
	private long allocated;
	@JsonProperty("utilization")
	private long utilization;
	@JsonProperty("name")
	private String name;
	@JsonProperty("size")
	private long size;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The allocated
	 */
	@JsonProperty("allocated")
	public long getAllocated() {
		return allocated;
	}

	/**
	 * 
	 * @param allocated
	 *            The allocated
	 */
	@JsonProperty("allocated")
	public void setAllocated(long allocated) {
		this.allocated = allocated;
	}

	public HeapInfo withAllocated(long allocated) {
		this.allocated = allocated;
		return this;
	}

	/**
	 * 
	 * @return The utilization
	 */
	@JsonProperty("utilization")
	public long getUtilization() {
		return utilization;
	}

	/**
	 * 
	 * @param utilization
	 *            The utilization
	 */
	@JsonProperty("utilization")
	public void setUtilization(long utilization) {
		this.utilization = utilization;
	}

	public HeapInfo withUtilization(long utilization) {
		this.utilization = utilization;
		return this;
	}

	/**
	 * 
	 * @return The name
	 */
	@JsonProperty("name")
	public String getName() {
		return name;
	}

	/**
	 * 
	 * @param name
	 *            The name
	 */
	@JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}

	public HeapInfo withName(String name) {
		this.name = name;
		return this;
	}

	/**
	 * 
	 * @return The size
	 */
	@JsonProperty("size")
	public long getSize() {
		return size;
	}

	/**
	 * 
	 * @param size
	 *            The size
	 */
	@JsonProperty("size")
	public void setSize(long size) {
		this.size = size;
	}

	public HeapInfo withSize(long size) {
		this.size = size;
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

	public HeapInfo withAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
		return this;
	}

}