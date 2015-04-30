package mil.nga.giat.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class ShortSerializationProvider implements
		FieldSerializationProviderSpi<Short>
{

	@Override
	public FieldReader<Short> getFieldReader() {
		return new ShortReader();
	}

	@Override
	public FieldWriter<Object, Short> getFieldWriter() {
		return new ShortWriter();
	}

	protected static class ShortReader implements
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

	protected static class ShortWriter implements
			FieldWriter<Object, Short>
	{
		@Override
		public byte[] writeField(
				final Short fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(2);
			buf.putShort(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Short fieldValue ) {
			return new byte[] {};
		}
	}
}
