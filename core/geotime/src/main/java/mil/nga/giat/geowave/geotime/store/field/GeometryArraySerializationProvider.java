package mil.nga.giat.geowave.geotime.store.field;

import mil.nga.giat.geowave.geotime.store.field.GeometrySerializationProvider.GeometryReader;
import mil.nga.giat.geowave.geotime.store.field.GeometrySerializationProvider.GeometryWriter;
import mil.nga.giat.geowave.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.store.data.field.FieldWriter;

import com.vividsolutions.jts.geom.Geometry;

public class GeometryArraySerializationProvider implements
		FieldSerializationProviderSpi<Geometry[]>
{
	@Override
	public FieldReader<Geometry[]> getFieldReader() {
		return new GeometryArrayReader();
	}

	@Override
	public FieldWriter<Object, Geometry[]> getFieldWriter() {
		return new GeometryArrayWriter();
	}

	private static class GeometryArrayReader extends
			VariableSizeObjectArrayReader<Geometry>
	{
		public GeometryArrayReader() {
			super(
					new GeometryReader());
		}
	}

	private static class GeometryArrayWriter extends
			VariableSizeObjectArrayWriter<Object, Geometry>
	{
		public GeometryArrayWriter() {
			super(
					new GeometryWriter());
		}
	}

}
