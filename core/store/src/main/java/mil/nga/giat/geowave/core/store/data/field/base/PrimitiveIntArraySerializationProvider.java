package mil.nga.giat.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class PrimitiveIntArraySerializationProvider implements
		FieldSerializationProviderSpi<int[]>
{
	@Override
	public FieldReader<int[]> getFieldReader() {
		return new PrimitiveIntArrayReader();
	}

	@Override
	public FieldWriter<Object, int[]> getFieldWriter() {
		return new PrimitiveIntArrayWriter();
	}

	private static class PrimitiveIntArrayReader implements
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

	private static class PrimitiveIntArrayWriter implements
			FieldWriter<Object, int[]>
	{
		@Override
		public byte[] writeField(
				final int[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(4 * fieldValue.length);
			for (final int value : fieldValue) {
				buf.putInt(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final int[] fieldValue ) {
			return new byte[] {};
		}
	}
}
