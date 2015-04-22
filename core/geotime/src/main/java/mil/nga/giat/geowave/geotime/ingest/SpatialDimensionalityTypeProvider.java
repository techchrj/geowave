package mil.nga.giat.geowave.geotime.ingest;

import mil.nga.giat.geowave.geotime.DimensionalityType;
import mil.nga.giat.geowave.ingest.IndexCompatibilityVisitor;
import mil.nga.giat.geowave.ingest.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.store.index.Index;

public class SpatialDimensionalityTypeProvider implements
		IngestDimensionalityTypeProviderSpi
{

	@Override
	public IndexCompatibilityVisitor getCompatibilityVisitor() {
		return new SpatialCompatibilityVisitor();
	}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Latitude and Longitude definitions.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just higher than spatial temporal so that the default
		// will be spatial over spatial-temporal
		return 10;
	}

	private static final class SpatialCompatibilityVisitor implements
			IndexCompatibilityVisitor
	{

		@Override
		public boolean isCompatible(
				final Index index ) {
			return DimensionalityType.SPATIAL_TEMPORAL.isCompatible(index);
		}

	}

}
