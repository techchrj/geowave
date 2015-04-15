package mil.nga.giat.geowave.accumulo.util;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

// TODO - do we need other types in addition to T... are generics being used correctly?
public class AccumuloKeyValuePairGenerator<T>
{
	private WritableDataAdapter<T> adapter;
	private Index index;
	private VisibilityWriter<T> visibilityWriter;

	public AccumuloKeyValuePairGenerator(
			WritableDataAdapter<T> adapter,
			Index index,
			VisibilityWriter<T> visibilityWriter ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.visibilityWriter = visibilityWriter;
	}

	public List<AccumuloKeyValuePair> constructKeyValuePairs(
			byte[] adapterId,
			T entry ) {

		List<AccumuloKeyValuePair> keyValuePairs = new ArrayList<AccumuloKeyValuePair>();

		DataStoreEntryInfo ingestInfo = AccumuloUtils.getIngestInfo(
				adapter,
				index,
				entry,
				visibilityWriter);

		List<ByteArrayId> rowIds = ingestInfo.getRowIds();
		List<FieldInfo> fieldInfoList = ingestInfo.getFieldInfo();

		for (ByteArrayId rowId : rowIds) {
			for (FieldInfo fieldInfo : fieldInfoList) {
				Key key = new Key(
						rowId.getBytes(),
						adapterId,
						fieldInfo.getDataValue().getId().getBytes(),
						fieldInfo.getVisibility(),
						System.currentTimeMillis()); // FIXME
				Value value = new Value(
						fieldInfo.getWrittenValue());
				AccumuloKeyValuePair kvp = new AccumuloKeyValuePair(
						key,
						value);
				keyValuePairs.add(kvp);
			}
		}

		return keyValuePairs;
	}

}
