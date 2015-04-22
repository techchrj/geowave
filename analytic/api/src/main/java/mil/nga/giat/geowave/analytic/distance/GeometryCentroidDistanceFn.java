package mil.nga.giat.geowave.analytic.distance;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Calculate distance between two geometries.
 * 
 * @see com.vividsolutions.jts.geom.Geometry
 * 
 */
public class GeometryCentroidDistanceFn implements
		DistanceFn<Geometry>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4340689267509659236L;
	private DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateEuclideanDistanceFn();

	public GeometryCentroidDistanceFn() {}

	public GeometryCentroidDistanceFn(
			final DistanceFn<Coordinate> coordinateDistanceFunction ) {
		super();
		this.coordinateDistanceFunction = coordinateDistanceFunction;
	}

	public DistanceFn<Coordinate> getCoordinateDistanceFunction() {
		return coordinateDistanceFunction;
	}

	public void setCoordinateDistanceFunction(
			final DistanceFn<Coordinate> coordinateDistanceFunction ) {
		this.coordinateDistanceFunction = coordinateDistanceFunction;
	}

	@Override
	public double measure(
			final Geometry x,
			final Geometry y ) {

		return coordinateDistanceFunction.measure(
				x.getCentroid().getCoordinate(),
				y.getCentroid().getCoordinate());
	}

}
