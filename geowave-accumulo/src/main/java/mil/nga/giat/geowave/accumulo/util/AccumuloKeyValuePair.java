package mil.nga.giat.geowave.accumulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class AccumuloKeyValuePair
{
	private final Key key;
	private final Value value;

	public AccumuloKeyValuePair(
			final Key key,
			final Value value ) {
		super();
		this.key = key;
		this.value = value;
	}

	public Key getKey() {
		return key;
	}

	public Value getValue() {
		return value;
	}

}
