package com.okmich.logging.app;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public final class HelloKafkaStream {

	public static void main(String[] args) {
		final String bootstrapServers = args.length > 0 ? args[0]
				: "quickstart.cloudera:9092";
		final Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
				"hello-kafka-logstream");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG,
				"hello-kafka-logstream-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		streamsConfiguration.put(
				StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()
						.getClass().getName());

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> logEventStream = builder.stream(args[1]);
		
		//perform logic on the value of the stream 
		KTable<String, String> logStreamKTable = null;  //put all your logic here

		logStreamKTable.to(Serdes.String(), Serdes.String(), args[2]);

		final KafkaStreams kStreams = new KafkaStreams(builder,
				streamsConfiguration);
		kStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				kStreams.close();
			}
		}));
	}
}
