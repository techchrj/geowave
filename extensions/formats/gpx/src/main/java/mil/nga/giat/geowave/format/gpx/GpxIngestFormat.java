package mil.nga.giat.geowave.format.gpx;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;

/**
 * This represents an ingest format plugin provider for GPX data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class GpxIngestFormat extends
		AbstractSimpleFeatureIngestFormat<GpxTrack>
{
	@Override
	protected AbstractSimpleFeatureIngestPlugin<GpxTrack> newPluginInstance() {
		return new GpxIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "gpx";
	}

	@Override
	public String getIngestFormatDescription() {
		return "xml files adhering to the schema of gps exchange format";
	}

}
