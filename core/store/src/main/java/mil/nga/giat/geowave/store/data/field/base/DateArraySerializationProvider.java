package mil.nga.giat.geowave.store.data.field.base;

import java.util.Date;

import mil.nga.giat.geowave.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.data.field.base.DateSerializationProvider.DateReader;
import mil.nga.giat.geowave.store.data.field.base.DateSerializationProvider.DateWriter;

public class DateArraySerializationProvider implements
		FieldSerializationProviderSpi<Date[]>
{
	@Override
	public FieldReader<Date[]> getFieldReader() {
		return new DateArrayReader();
	}

	@Override
	public FieldWriter<Object, Date[]> getFieldWriter() {
		return new DateArrayWriter();
	}

	private static class DateArrayReader extends
			FixedSizeObjectArrayReader<Date>
	{
		public DateArrayReader() {
			super(
					new DateReader());
		}
	}

	private static class DateArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Date>
	{
		public DateArrayWriter() {
			super(
					new DateWriter());
		}
	}
}
