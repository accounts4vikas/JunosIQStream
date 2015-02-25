/home/ubuntu/spark-1.2.0-bin-hadoop2.4/bin/spark-submit \
		--class net.juniper.iq.batch.BatchDemo \
		--master spark://ip-10-10-0-126:7077 \
		--files ./log4j.properties \
		./AWSJunosIQStream-0.0.1-SNAPSHOT.jar \
		$@
