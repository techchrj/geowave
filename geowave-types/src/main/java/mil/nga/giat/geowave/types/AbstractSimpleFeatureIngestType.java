package mil.nga.giat.geowave.types;

import mil.nga.giat.geowave.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

abstract public class AbstractSimpleFeatureIngestType<I> implements
		IngestFormatPluginProviderSpi<I, SimpleFeature>
{
	protected final CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();
	protected AbstractSimpleFeatureIngestPlugin<I> myInstance;

	private synchronized AbstractSimpleFeatureIngestPlugin<I> getInstance() {
		if (myInstance == null) {
			myInstance = newPluginInstance();
			myInstance.setFilterProvider(cqlFilterOptionProvider);
		}
		return myInstance;
	}

	abstract protected AbstractSimpleFeatureIngestPlugin<I> newPluginInstance();

	@Override
	public StageToHdfsPlugin<I> getStageToHdfsPlugin() {
		return getInstance();
	}

	@Override
	public IngestFromHdfsPlugin<I, SimpleFeature> getIngestFromHdfsPlugin() {
		return getInstance();
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return getInstance();
	}

	@Override
	public IngestFormatOptionProvider getIngestTypeOptionProvider() {
		return cqlFilterOptionProvider;
	}

}
