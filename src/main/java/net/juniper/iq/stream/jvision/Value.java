package net.juniper.iq.stream.jvision;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)

@JsonPropertyOrder({ "heap_info" })
public class Value {

	@JsonProperty("heap_info")
	private List<HeapInfo> heapInfo = new ArrayList<HeapInfo>();
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The heapInfo
	 */
	@JsonProperty("heap_info")
	public List<HeapInfo> getHeapInfo() {
		return heapInfo;
	}

	/**
	 * 
	 * @param heapInfo
	 *            The heap_info
	 */
	@JsonProperty("heap_info")
	public void setHeapInfo(List<HeapInfo> heapInfo) {
		this.heapInfo = heapInfo;
	}

	public Value withHeapInfo(List<HeapInfo> heapInfo) {
		this.heapInfo = heapInfo;
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

	public Value withAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
		return this;
	}

}
