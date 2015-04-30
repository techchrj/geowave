package mil.nga.giat.geowave.core.store.data.field.base;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class PrimitiveByteArraySerializationProvider implements
		FieldSerializationProviderSpi<byte[]>
{
	@Override
	public FieldReader<byte[]> getFieldReader() {
		return new PrimitiveByteArrayReader();
	}

	@Override
	public FieldWriter<Object, byte[]> getFieldWriter() {
		return new PrimitiveByteArrayWriter();
	}

	private static class PrimitiveByteArrayReader implements
			FieldReader<byte[]>
	{
		@Override
		public byte[] readField(
				final byte[] fieldData ) {
			return Arrays.copyOf(
					fieldData,
					fieldData.length);
		}
	}

	private static class PrimitiveByteArrayWriter implements
			FieldWriter<Object, byte[]>
	{
		@Override
		public byte[] writeField(
				final byte[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return fieldValue;
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final byte[] fieldValue ) {
			return new byte[] {};
		}
	}
}
