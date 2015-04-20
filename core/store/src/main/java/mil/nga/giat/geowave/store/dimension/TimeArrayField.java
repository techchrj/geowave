package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;

public class TimeArrayField extends
		ArrayField<Time> implements
		DimensionField<ArrayWrapper<Time>>
{
	private ArrayAdapter<Time> adapter;

	public TimeArrayField(
			final DimensionField<Time> elementField ) {
		super(
				elementField);
		adapter = new ArrayAdapter<Time>(
				new FixedSizeObjectArrayReader(
						elementField.getReader()),
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	public TimeArrayField() {}

	@Override
	public FieldWriter<?, ArrayWrapper<Time>> getWriter() {
		return adapter;
	}

	@Override
	public FieldReader<ArrayWrapper<Time>> getReader() {
		return adapter;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		super.fromBinary(bytes);
		adapter = new ArrayAdapter<Time>(
				new FixedSizeObjectArrayReader(
						elementField.getReader()),
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}
}
