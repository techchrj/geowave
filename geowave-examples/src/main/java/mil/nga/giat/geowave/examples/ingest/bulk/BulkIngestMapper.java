package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.accumulo.util.AccumuloKeyValuePair;
import mil.nga.giat.geowave.accumulo.util.AccumuloKeyValuePairGenerator;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;

public class BulkIngestMapper extends
		Mapper<LongWritable, SimpleFeature, Key, Value>
// FIXME key, value input types!
{
	private WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
			GeonamesSimpleFeatureType.getInstance());
	private final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
			new UnconstrainedVisibilityHandler<SimpleFeature, Object>());
	private AccumuloKeyValuePairGenerator<SimpleFeature> generator = new AccumuloKeyValuePairGenerator<SimpleFeature>(
			adapter,
			index,
			visibilityWriter);
	private List<AccumuloKeyValuePair> keyValuePairs;

	@Override
	protected void map(
			LongWritable key,
			SimpleFeature value,
			Context context )
			throws IOException,
			InterruptedException {

		// build Geowave-formatted Accumulo [Key,Value] pairs
		keyValuePairs = generator.constructKeyValuePairs(
				adapter.getAdapterId().getBytes(),
				value);

		// output each [Key,Value] pair to shuffle-and-sort phase where we rely
		// on MapReduce to sort by Key
		for (AccumuloKeyValuePair accumuloKeyValuePair : keyValuePairs) {
			context.write(
					accumuloKeyValuePair.getKey(),
					accumuloKeyValuePair.getValue());
		}
	}

}
