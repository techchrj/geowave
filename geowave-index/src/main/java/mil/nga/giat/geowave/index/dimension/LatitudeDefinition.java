package mil.nga.giat.geowave.index.dimension;

/**
 * The Latitude Definition class is a convenience class used to define a
 * dimension which is associated with the Y axis on a Cartesian plane.
 * 
 * Minimum bounds = -90 and maximum bounds = 90
 * 
 */
public class LatitudeDefinition extends
		BasicDimensionDefinition
{

	/**
	 * Convenience constructor used to construct a simple latitude dimension
	 * object which sits on a Cartesian plane.
	 * 
	 */
	public LatitudeDefinition() {
		this(
				false);
	}

	/**
	 * Convenience constructor used to construct a simple latitude dimension
	 * object which sits on a Cartesian plane. You can pass in a flag to use
	 * half the range if you want square SFC IDs in decimal degree latitudes and
	 * longitudes
	 * 
	 */
	public LatitudeDefinition(
			final boolean useHalfRange ) {
		super(
				useHalfRange ? -180 : -90,
				useHalfRange ? 180 : 90);
	}

	@Override
	protected double clamp(
			final double x ) {
		// continue to clamp values between -90 and 90 regardless of whether
		// we're using half the range
		return clamp(
				x,
				-90,
				90);
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			(byte) (((min > -180) && (max < 180)) ? 0 : 1)
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == (byte) 1) {
				// this implies we just want to use half the range
				min = -180;
				max = 180;
			}
		}
	}
}
