package mil.nga.giat.geowave.ingest.kafka;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.local.AbstractLocalFileDriver;

/**
 * This class actually executes the staging of data to HDFS based on the
 * available type plugin providers that are discovered through SPI.
 */
public class StageToKafkaDriver<T extends SpecificRecordBase> extends
		AbstractLocalFileDriver<StageToKafkaPlugin<?>, StageKafkaData<T>>
{
	private final static Logger LOGGER = Logger.getLogger(StageToKafkaDriver.class);
	private KafkaCommandLineOptions kafkaOptions;
	private Producer<String, T> producer;

	public StageToKafkaDriver(
			final String operation ) {
		super(
				operation);

	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		kafkaOptions = KafkaCommandLineOptions.parseOptions(commandLine);
		super.parseOptionsInternal(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		KafkaCommandLineOptions.applyOptions(allOptions);
		super.applyOptionsInternal(allOptions);

	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final StageToKafkaPlugin<?> plugin,
			final StageKafkaData<T> runData ) {

		if (producer == null) {
			Properties props = KafkaCommandLineOptions.getProperties();
			props.put(
					"serializer.class",
					"mil.nga.giat.geowave.ingest.kafka.AvroKafkaEncoder");
			props.put(
					"message.max.bytes",
					"5000000");

			producer = new Producer<String, T>(
					new ProducerConfig(
							props));
		}

		// System.out.println("working on " + file.getAbsolutePath());
		T something = plugin.toAvroObject(file);
		KeyedMessage<String, T> data = new KeyedMessage<String, T>(
				kafkaOptions.getKafkaTopic(),
				something);
		producer.send(data);
		// System.out.println("sent to kafka " + file.getAbsolutePath());

	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders ) {

		// first collect the stage to kafka plugins
		final Map<String, StageToKafkaPlugin<?>> stageToKafkaPlugins = new HashMap<String, StageToKafkaPlugin<?>>();
		for (final IngestTypePluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			StageToKafkaPlugin<?> stageToKafkaPlugin = null;
			try {
				stageToKafkaPlugin = pluginProvider.getStageToKafkaPlugin();

				if (stageToKafkaPlugin == null) {
					LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestTypeName() + "' does not support staging to HDFS");
					continue;
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn(
						"Plugin provider '" + pluginProvider.getIngestTypeName() + "' does not support staging to HDFS",
						e);
				continue;
			}
			stageToKafkaPlugins.put(
					pluginProvider.getIngestTypeName(),
					stageToKafkaPlugin);
		}

		try {
			final StageKafkaData<T> runData = new StageKafkaData<T>();
			processInput(
					stageToKafkaPlugins,
					runData);
			runData.close();
			producer.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
