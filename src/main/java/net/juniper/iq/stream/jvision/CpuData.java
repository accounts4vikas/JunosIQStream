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
@JsonPropertyOrder({ "header", "value", "metadata" })
public class CpuData {

	@JsonProperty("header")
	private Header header;
	@JsonProperty("value")
	private Value value;
	@JsonProperty("metadata")
	private Metadata metadata;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The header
	 */
	@JsonProperty("header")
	public Header getHeader() {
		return header;
	}

	/**
	 * 
	 * @param header
	 *            The header
	 */
	@JsonProperty("header")
	public void setHeader(Header header) {
		this.header = header;
	}

	public CpuData withHeader(Header header) {
		this.header = header;
		return this;
	}

	/**
	 * 
	 * @return The value
	 */
	@JsonProperty("value")
	public Value getValue() {
		return value;
	}

	/**
	 * 
	 * @param value
	 *            The value
	 */
	@JsonProperty("value")
	public void setValue(Value value) {
		this.value = value;
	}

	public CpuData withValue(Value value) {
		this.value = value;
		return this;
	}

	/**
	 * 
	 * @return The metadata
	 */
	@JsonProperty("metadata")
	public Metadata getMetadata() {
		return metadata;
	}

	/**
	 * 
	 * @param metadata
	 *            The metadata
	 */
	@JsonProperty("metadata")
	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}

	public CpuData withMetadata(Metadata metadata) {
		this.metadata = metadata;
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

	public CpuData withAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
		return this;
	}

}