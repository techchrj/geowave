package mil.nga.giat.geowave.store.data.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.GeometryUtils;

import org.apache.commons.lang3.ArrayUtils;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class contains all of the primitive reader field types supported
 * 
 */
public class BasicReader
{


	public static class ShortReader implements
			FieldReader<Short>
	{

		@Override
		public Short readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 2)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getShort();
		}

	}

	public static class PrimitiveShortArrayReader implements
			FieldReader<short[]>
	{

		@Override
		public short[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 2)) {
				return null;
			}
			final ShortBuffer buff = ByteBuffer.wrap(
					fieldData).asShortBuffer();
			final short[] result = new short[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class PrimitiveFloatArrayReader implements
			FieldReader<float[]>
	{

		@Override
		public float[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			final FloatBuffer buff = ByteBuffer.wrap(
					fieldData).asFloatBuffer();
			final float[] result = new float[buff.remaining()];
			buff.get(result);
			return result;
		}
	}


	public static class PrimitiveIntArrayReader implements
			FieldReader<int[]>
	{

		@Override
		public int[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			final IntBuffer buff = ByteBuffer.wrap(
					fieldData).asIntBuffer();
			final int[] result = new int[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class PrimitiveLongArrayReader implements
			FieldReader<long[]>
	{

		@Override
		public long[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			final LongBuffer buff = ByteBuffer.wrap(
					fieldData).asLongBuffer();
			final long[] result = new long[buff.remaining()];
			buff.get(result);
			return result;
		}
	}


	public static class StringReader implements
			FieldReader<String>
	{

		@Override
		public String readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return StringUtils.stringFromBinary(fieldData);
		}

	}


}
