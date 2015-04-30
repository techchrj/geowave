package mil.nga.giat.geowave.format.geotools.raster;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

import org.opengis.coverage.grid.GridCoverage;

/**
 * This represents an ingest format plugin provider for GeoTools grid coverage
 * (raster) formats. It currently only supports ingesting data directly from a
 * local file system into GeoWave.
 */
public class GeoToolsRasterDataStoreIngestFormat implements
		IngestFormatPluginProviderSpi<Object, GridCoverage>
{

	@Override
	public StageToHdfsPlugin<Object> getStageToHdfsPlugin()
			throws UnsupportedOperationException {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools raster files cannot be ingested from HDFS");
	}

	@Override
	public IngestFromHdfsPlugin<Object, GridCoverage> getIngestFromHdfsPlugin()
			throws UnsupportedOperationException {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools raster files cannot be ingested from HDFS");
	}

	@Override
	public LocalFileIngestPlugin<GridCoverage> getLocalFileIngestPlugin()
			throws UnsupportedOperationException {
		return new GeoToolsRasterDataStoreIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "geotools-raster";
	}

	@Override
	public String getIngestFormatDescription() {
		return "all file-based raster formats supported within geotools";
	}

	@Override
	public IngestFormatOptionProvider getIngestFormatOptionProvider() {
		// no custom options are provided
		return null;
	}

}
