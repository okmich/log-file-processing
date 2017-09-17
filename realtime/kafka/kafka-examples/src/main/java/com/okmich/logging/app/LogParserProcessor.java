package com.okmich.logging.app;

import static com.okmich.logging.app.util.Util.parseLog;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

public final class LogParserProcessor {

	private static final Logger LOG = Logger.getLogger(LogParserProcessor.class
			.getName());

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
				"hello-kafka-logstream");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG,
				"logstream-parser-client");
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
		KStream<String, String> logStreamKTable = logEventStream
				.mapValues(new ValueMapper<String, String>() {

					@Override
					public String apply(String value) {
						return parseLog(value);
					}
				});
		KStream<String, String> filteredStream = logStreamKTable
				.filter(new Predicate<String, String>() {

					@Override
					public boolean test(String key, String value) {
						return !value.isEmpty();
					}
				});

		filteredStream.to(Serdes.String(), Serdes.String(), args[2]);

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
