mvn compile
mvn package
spark-submit  --files /Users/vsangwan/sbox/softwares/spark-1.2.0-bin-hadoop2.4/conf/log4j.properties \
              --class net.juniper.iq.stream.StreamingDemo \
             ./target/JunosIQStream-0.0.1-SNAPSHOT.jar \
             $@
