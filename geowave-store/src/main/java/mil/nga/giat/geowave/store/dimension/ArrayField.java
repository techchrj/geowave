package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

public class ArrayField<T extends CommonIndexValue> implements
		DimensionField<ArrayWrapper<T>>
{
	protected DimensionField<T> elementField;

	public ArrayField(
			final DimensionField<T> elementField ) {
		this.elementField = elementField;
	}

	public ArrayField() {}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(elementField);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		elementField = PersistenceUtils.fromBinary(
				bytes,
				DimensionField.class);
	}

	@Override
	public double getRange() {
		return elementField.getRange();
	}

	@Override
	public double normalize(
			final double value ) {
		return elementField.normalize(value);
	}

	@Override
	public double denormalize(
			final double value ) {
		return elementField.denormalize(value);
	}

	@Override
	public ByteArrayId getFieldId() {
		return elementField.getFieldId();
	}

	@Override
	public BinRange[] getNormalizedRanges(
			final NumericData range ) {
		return elementField.getNormalizedRanges(range);
	}

	@Override
	public NumericRange getDenormalizedRange(
			final BinRange range ) {
		return elementField.getDenormalizedRange(range);
	}

	@Override
	public NumericData getNumericData(
			final ArrayWrapper<T> dataElement ) {
		Double min = null, max = null;
		for (final T element : dataElement.getArray()) {
			final NumericData data = elementField.getNumericData(element);
			if ((min == null) || (max == null)) {
				min = data.getMin();
				max = data.getMax();
			}
			else {
				min = Math.min(
						min,
						data.getMin());
				max = Math.max(
						max,
						data.getMax());
			}
		}
		if ((min == null) || (max == null)) {
			return null;
		}
		if (min.equals(max)) {
			return new NumericValue(
					min);
		}
		return new NumericRange(
				min,
				max);
	}

	@Override
	public FieldWriter<?, ArrayWrapper<T>> getWriter() {
		return new ArrayAdapter(
				elementField.getReader(),
				elementField.getWriter());
	}

	@Override
	public FieldReader<ArrayWrapper<T>> getReader() {
		return new ArrayAdapter(
				elementField.getReader(),
				elementField.getWriter());
	}

	@Override
	public NumericDimensionDefinition getBaseDefinition() {
		return elementField.getBaseDefinition();
	}

	@Override
	public int getFixedBinIdSize() {
		return elementField.getFixedBinIdSize();
	}

	@Override
	public NumericRange getBounds() {
		return elementField.getBounds();
	}

	@Override
	public NumericData getFullRange() {
		// TODO Auto-generated method stub
		return null;
	}

}
