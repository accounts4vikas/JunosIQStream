package net.juniper.iq.stream.functions;

import java.io.IOException;

import net.juniper.iq.stream.jvision.CpuData;
import net.juniper.iq.stream.jvision.HeapInfo;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;


import com.fasterxml.jackson.databind.ObjectMapper;

public class HeapInfoFlatMapFunction implements FlatMapFunction<String, HeapInfo> {
    private static final long serialVersionUID = 42l;
    private final ObjectMapper mapper = new ObjectMapper();
    
	public Iterable<HeapInfo> call(String jsonInput) throws Exception {
		try
        {
			CpuData cpuData = mapper.readValue(jsonInput, CpuData.class);

            return cpuData.getValue().getHeapInfo();
        }
        catch (IOException ex)
        {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("IO error while filtering tweets", ex);
            LOG.trace(null, ex);
        }
        return null;	}

}
