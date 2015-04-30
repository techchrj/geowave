package mil.nga.giat.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class PrimitiveLongArraySerializationProvider implements
		FieldSerializationProviderSpi<long[]>
{
	@Override
	public FieldReader<long[]> getFieldReader() {
		return new PrimitiveLongArrayReader();
	}

	@Override
	public FieldWriter<Object, long[]> getFieldWriter() {
		return new PrimitiveLongArrayWriter();
	}

	private static class PrimitiveLongArrayReader implements
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

	private static class PrimitiveLongArrayWriter implements
			FieldWriter<Object, long[]>
	{
		public PrimitiveLongArrayWriter() {
			super();
		}

		@Override
		public byte[] writeField(
				final long[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(8 * fieldValue.length);
			for (final long value : fieldValue) {
				buf.putLong(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final long[] fieldValue ) {
			return new byte[] {};
		}
	}

}
