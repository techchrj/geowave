package mil.nga.giat.geowave.format.geolife;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestType;
import mil.nga.giat.geowave.core.ingest.hdfs.HdfsFile;

/**
 * This represents an ingest type plugin provider for GeoLife data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class GeoLifeIngestType extends
		AbstractSimpleFeatureIngestType<HdfsFile>
{

	@Override
	protected AbstractSimpleFeatureIngestPlugin<HdfsFile> newPluginInstance() {
		return new GeoLifeIngestPlugin();
	}

	@Override
	public String getIngestTypeName() {
		return "geolife";
	}

	@Override
	public String getIngestTypeDescription() {
		return "files from Microsoft Research GeoLife trajectory data set";
	}
}
