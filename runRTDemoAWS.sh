/home/ubuntu/spark-1.2.1-bin-hadoop2.4/bin/spark-submit \
		--class net.juniper.iq.stream.StreamingDemo \
		--master spark://ip-10-10-0-10:7077 \
		--files ./log4j.properties \
        --total-executor-cores 3 \
        --executor-memory 4g \
		./AWSJunosIQStream-0.0.1-SNAPSHOT.jar \
		$@
