package mil.nga.giat.geowave.analytic.extract;

import java.util.Iterator;

import mil.nga.giat.geowave.vector.adapter.FeatureGeometryHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.ReferenceIdentifier;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * Extract a Geometry from a Simple Feature.
 * 
 */
public class SimpleFeatureGeometryExtractor extends
		EmptyDimensionExtractor<SimpleFeature> implements
		DimensionExtractor<SimpleFeature>
{
	@Override
	public Geometry getGeometry(
			final SimpleFeature anObject ) {
		final FeatureGeometryHandler handler = new FeatureGeometryHandler(
				anObject.getDefaultGeometryProperty().getDescriptor());
		final Geometry geometry = handler.toIndexValue(
				anObject).getGeometry();
		final int srid = getSRID(anObject);
		geometry.setSRID(srid);
		return geometry;
	}

	protected static int getSRID(
			final SimpleFeature geometryFeature ) {
		final ReferenceIdentifier id = getFirst(geometryFeature.getDefaultGeometryProperty().getDescriptor().getCoordinateReferenceSystem().getIdentifiers());
		if (id == null) {
			return 4326;
		}
		return Integer.parseInt(id.getCode());
	}

	protected static final <T> ReferenceIdentifier getFirst(
			final Iterable<ReferenceIdentifier> iterable ) {
		if (iterable == null) return null;
		final Iterator<ReferenceIdentifier> it = iterable.iterator();
		if (it.hasNext()) {
			final ReferenceIdentifier id = it.next();
			if ("EPSG".equals(id.getCodeSpace())) {
				return id;
			}
		}
		return null;
	}

	@Override
	public String getGroupID(
			SimpleFeature anObject ) {
		Object v = anObject.getAttribute("GroupID");
		return v == null ? null : v.toString();
	}

}
