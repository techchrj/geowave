package mil.nga.giat.geowave.geotime.ingest;

import mil.nga.giat.geowave.geotime.DimensionalityType;
import mil.nga.giat.geowave.ingest.IndexCompatibilityVisitor;
import mil.nga.giat.geowave.ingest.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.store.index.Index;

public class SpatialTemporalDimensionalityTypeProvider implements
		IngestDimensionalityTypeProviderSpi
{
	@Override
	public IndexCompatibilityVisitor getCompatibilityVisitor() {
		return new SpatialTemporalIndexCompatibilityVisitor();
	}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial-temporal";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Latitude, Longitude, and Time definitions.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just lower than spatial so that the default
		// will be spatial over spatial-temporal
		return 5;
	}

	private static class SpatialTemporalIndexCompatibilityVisitor implements
			IndexCompatibilityVisitor
	{

		@Override
		public boolean isCompatible(
				final Index index ) {
			return DimensionalityType.SPATIAL.isCompatible(index);
		}

	}
}
