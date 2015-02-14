package net.juniper.iq.stream;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import net.juniper.iq.stream.jvision.CpuData;
import net.juniper.iq.stream.jvision.HeapInfo;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonProducer {
	private final Logger LOG = Logger.getLogger(this.getClass());
	ObjectMapper mapper = new ObjectMapper();

	private CpuData readJsonLineData() {

		CpuData cpuData;

		try {
			InputStream inStream = this.getClass().getClassLoader()
					.getResourceAsStream("json_input.json");
			cpuData = mapper.readValue(inStream, CpuData.class);

			return cpuData;
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	private String createNewJsonData(CpuData cpuData, int i) {
		String outJson;
		try {
			Random randomGenerator = new Random();
			int randKernelUtilization = randomGenerator.nextInt(100);
			int randomLanBUtilization = randomGenerator.nextInt(100);
			int randomISSUUtilization = randomGenerator.nextInt(100);
			int randBlobUtilization = randomGenerator.nextInt(100);
			
			List<HeapInfo> heapInfoList = cpuData.getValue().getHeapInfo();
			Iterator<HeapInfo> itr = heapInfoList.iterator();
			while(itr.hasNext()) {
				HeapInfo heapInfo = itr.next();
				//heapInfo.setUtilization(randomIntUtilization);
				if(heapInfo.getName().equals("Kernel")) {
					heapInfo.setUtilization(randKernelUtilization);
				} else if (heapInfo.getName().equals("ISSU scratch")) {
					heapInfo.setUtilization(randomISSUUtilization);
				} else if (heapInfo.getName().equals("Blob")) {
					heapInfo.setUtilization(randBlobUtilization);
				} else {
					heapInfo.setUtilization(randomLanBUtilization);
				}
			}
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date currentDateTime = new Date();
			cpuData.getMetadata().setLocaltime(simpleDateFormat.format(currentDateTime));
					
			outJson = mapper.writeValueAsString(cpuData);
			return outJson;
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
	

	private Producer<String, String> getKafkaProducer() {
		java.util.Properties props = new java.util.Properties();
		//props.put("metadata.broker.list", "10.10.0.182:9092");
		props.put("metadata.broker.list", "localhost:9092");
		
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		// props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		return producer;
	}

	private void sendJsonToKafka(Producer<String, String> producer, String topic, String jsonData) {
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				topic, jsonData);
		producer.send(data);
	}

	public static void main(String[] args) throws InterruptedException {
		String topic = "JunosIQStream";

		KafkaJsonProducer kafkaProducer = new KafkaJsonProducer();

		Producer<String, String> producer = kafkaProducer.getKafkaProducer();

		CpuData cpuData = kafkaProducer.readJsonLineData();

		String tokenizedJsonData;

		for (int i = 1; i <= 1000; i++) {
			tokenizedJsonData = kafkaProducer.createNewJsonData(cpuData, i);
			System.out.println(tokenizedJsonData);
			
			kafkaProducer.sendJsonToKafka(producer, topic, tokenizedJsonData);


			if(i % 10 == 0) {
				Thread.sleep(500);
			}
		}

		producer.close();
	}

}
