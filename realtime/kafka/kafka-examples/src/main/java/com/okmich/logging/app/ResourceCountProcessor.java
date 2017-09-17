package com.okmich.logging.app;

import static com.okmich.logging.app.util.Util.getResourceValue;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public final class ResourceCountProcessor {

	private static final Logger LOG = Logger
			.getLogger(ResourceCountProcessor.class.getName());

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("Usage:: LogParserProcessor bootstrap_server incoming-topic outgoing-topic");
			System.exit(-1);
		}

		LOG.log(Level.INFO, "LogParserProcess from {0} to {1}", new Object[] {
				args[1], args[2] });

		final Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
				"resource-counter-logstream");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG,
				"logstream-resource-counter-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				args[0]);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		streamsConfiguration.put(
				StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()
						.getClass().getName());

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> logEventStream = builder.stream(args[1]);

		// perform logic on the value of the stream
		KTable<String, Long> logStreamKTable = logEventStream
				.mapValues(new ValueMapper<String, String>() {

					@Override
					public String apply(String arg0) {

						return getResourceValue(arg0);
					}
				}).groupBy(new KeyValueMapper<String, String, String>() {

					@Override
					public String apply(String key, String value) {
						LOG.info(value);
						// TODO Auto-generated method stub
						return value;
					}
				}).count();

		//change the long to string and write to topic
		logStreamKTable.mapValues(new ValueMapper<Long, String>() {

			@Override
			public String apply(Long arg0) {
				// TODO Auto-generated method stub
				return arg0.toString();
			}
		}).to(Serdes.String(), Serdes.String(), args[2]);

		final KafkaStreams kStreams = new KafkaStreams(builder,
				streamsConfiguration);

		LOG.info("Starting.....");
		kStreams.start();

		LOG.info("Adding shutdown hook.....");
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				kStreams.close();
			}
		}));
	}
	// java -cp target/kafka-examples-0.1-SNAPSHOT.jar
	// com.okmich.logging.app.LogParserProcessor quickstart.cloudera:9092
	// log-in-gateway processed-logs
}
