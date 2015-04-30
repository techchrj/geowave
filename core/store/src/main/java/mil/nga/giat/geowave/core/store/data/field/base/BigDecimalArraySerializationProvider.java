package mil.nga.giat.geowave.core.store.data.field.base;

import java.math.BigDecimal;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.BigDecimalSerializationProvider.BigDecimalReader;
import mil.nga.giat.geowave.core.store.data.field.base.BigDecimalSerializationProvider.BigDecimalWriter;

public class BigDecimalArraySerializationProvider implements
		FieldSerializationProviderSpi<BigDecimal[]>
{
	@Override
	public FieldReader<BigDecimal[]> getFieldReader() {
		return new BigDecimalArrayReader();
	}

	@Override
	public FieldWriter<Object, BigDecimal[]> getFieldWriter() {
		return new BigDecimalArrayWriter();
	}

	private static class BigDecimalArrayReader extends
			VariableSizeObjectArrayReader<BigDecimal>
	{
		public BigDecimalArrayReader() {
			super(
					new BigDecimalReader());
		}
	}

	private static class BigDecimalArrayWriter extends
			VariableSizeObjectArrayWriter<Object, BigDecimal>
	{
		public BigDecimalArrayWriter() {
			super(
					new BigDecimalWriter());
		}
	}
}
