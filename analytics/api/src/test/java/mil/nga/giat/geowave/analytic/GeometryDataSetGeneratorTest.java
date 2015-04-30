package mil.nga.giat.geowave.analytic;

import static org.junit.Assert.assertEquals;
import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class GeometryDataSetGeneratorTest
{

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate
														// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Geometry.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		return new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());
	}

	@Test
	public void test() {
		final GeometryDataSetGenerator dataGenerator = new GeometryDataSetGenerator(
				new FeatureCentroidDistanceFn(),
				getBuilder());
		Geometry region = dataGenerator.getBoundingRegion();
		Coordinate[] coordinates = region.getBoundary().getCoordinates();
		assertEquals(
				5,
				coordinates.length);
		assertEquals(
				"POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))",
				region.toString());
	}

}
