package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

import org.apache.commons.lang3.ArrayUtils;

public class ByteArraySerializationProvider implements
		FieldSerializationProviderSpi<Byte[]>
{
	@Override
	public FieldReader<Byte[]> getFieldReader() {
		return new ByteArrayReader();
	}

	@Override
	public FieldWriter<Object, Byte[]> getFieldWriter() {
		return new ByteArrayWriter();
	}

	public static class ByteArrayReader implements
			FieldReader<Byte[]>
	{
		@Override
		public Byte[] readField(
				final byte[] fieldData ) {
			return ArrayUtils.toObject(fieldData);
		}
	}

	public static class ByteArrayWriter implements
			FieldWriter<Object, Byte[]>
	{
		@Override
		public byte[] writeField(
				final Byte[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return ArrayUtils.toPrimitive(fieldValue);
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Byte[] fieldValue ) {
			return new byte[] {};
		}
	}
}
