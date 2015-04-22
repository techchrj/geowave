package mil.nga.giat.geowave.format.gpx;

import mil.nga.giat.geowave.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.vector.ingest.AbstractSimpleFeatureIngestType;


/**
 * This represents an ingest type plugin provider for GPX data. It will support
 * ingesting directly from a local file system or staging data from a local
 * files system and ingesting into GeoWave using a map-reduce job.
 */
public class GpxIngestType extends
		AbstractSimpleFeatureIngestType<GpxTrack>
{
	@Override
	protected AbstractSimpleFeatureIngestPlugin<GpxTrack> newPluginInstance() {
		return new GpxIngestPlugin();
	}

	@Override
	public String getIngestTypeName() {
		return "gpx";
	}

	@Override
	public String getIngestTypeDescription() {
		return "xml files adhering to the schema of gps exchange format";
	}

}
