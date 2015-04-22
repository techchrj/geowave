package mil.nga.giat.geowave.geotime.store.field;

import java.util.Calendar;

import mil.nga.giat.geowave.geotime.store.field.CalendarSerializationProvider.CalendarReader;
import mil.nga.giat.geowave.geotime.store.field.CalendarSerializationProvider.CalendarWriter;
import mil.nga.giat.geowave.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.store.data.field.FieldWriter;

public class CalendarArraySerializationProvider implements
		FieldSerializationProviderSpi<Calendar[]>
{
	@Override
	public FieldReader<Calendar[]> getFieldReader() {
		return new CalendarArrayReader();
	}

	@Override
	public FieldWriter<Object, Calendar[]> getFieldWriter() {
		return new CalendarArrayWriter();
	}

	private static class CalendarArrayReader extends
			FixedSizeObjectArrayReader<Calendar>
	{
		public CalendarArrayReader() {
			super(
					new CalendarReader());
		}
	}

	private static class CalendarArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Calendar>
	{
		public CalendarArrayWriter() {
			super(
					new CalendarWriter());
		}
	}

}
