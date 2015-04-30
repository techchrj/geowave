package mil.nga.giat.geowave.analytic;

import java.io.IOException;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Project a n-dimensional item into a two-dimensional polygon for convex hull
 * construction.
 * 
 * @param <T>
 */
public interface Projection<T>
{
	public Geometry getProjection(
			T anItem );

	public void initialize(
			ConfigurationWrapper context )
			throws IOException;
}
