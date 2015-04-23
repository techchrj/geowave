package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.accumulo.util.AccumuloKeyValuePair;
import mil.nga.giat.geowave.accumulo.util.AccumuloKeyValuePairGenerator;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class BulkIngestMapper extends
		Mapper<Text, Text, Key, Value>
{
	private static final String FEATURE_NAME = "GeonamesPoint";
	private static enum Counters {
		FEATURE_ID_COUNTER
	};
	private SimpleFeatureType geonamesPointType;
	private SimpleFeatureBuilder builder;
	private WritableDataAdapter<SimpleFeature> adapter;
	private final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
			new UnconstrainedVisibilityHandler<SimpleFeature, Object>());
	private AccumuloKeyValuePairGenerator<SimpleFeature> generator;
	private List<AccumuloKeyValuePair> keyValuePairs;
	private SimpleFeature simpleFeature;
	private String[] geonamesEntryTokens;
	private double longitude;
	private double latitude;
	private String location;

	@Override
	protected void setup(
			Mapper<Text, Text, Key, Value>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		geonamesPointType = createGeonamesPointType();
		adapter = new FeatureDataAdapter(
				geonamesPointType);
		builder = new SimpleFeatureBuilder(
				geonamesPointType);
		generator = new AccumuloKeyValuePairGenerator<SimpleFeature>(
				adapter,
				index,
				visibilityWriter);
		// TODO additional setup?
	}

	@Override
	protected void map(
			Text key,
			Text value,
			Mapper<Text, Text, Key, Value>.Context context )
			throws IOException,
			InterruptedException {

		context.getCounter(
				Counters.FEATURE_ID_COUNTER).increment(
				1);

		geonamesEntryTokens = value.toString().split(
				"\\t"); // Exported Geonames entries are tab-delimited
		location = geonamesEntryTokens[1];
		latitude = Double.parseDouble(geonamesEntryTokens[4]);
		longitude = Double.parseDouble(geonamesEntryTokens[5]);

		simpleFeature = buildSimpleFeature(
				Long.toString(context.getCounter(
						Counters.FEATURE_ID_COUNTER).getValue()),
				longitude,
				latitude,
				location);

		keyValuePairs = generator.constructKeyValuePairs(
				adapter.getAdapterId().getBytes(),
				simpleFeature);

		for (AccumuloKeyValuePair accumuloKeyValuePair : keyValuePairs) {
			context.write(
					accumuloKeyValuePair.getKey(),
					accumuloKeyValuePair.getValue());
		}
	}

	private SimpleFeatureType createGeonamesPointType() {

		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();

		sftBuilder.setName(FEATURE_NAME);

		sftBuilder.add(atBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		sftBuilder.add(atBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Latitude"));
		sftBuilder.add(atBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Longitude"));
		sftBuilder.add(atBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"Location"));

		return sftBuilder.buildFeatureType();
	}

	private SimpleFeature buildSimpleFeature(
			String featureId,
			double longitude,
			double latitude,
			String location ) {

		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						longitude,
						latitude)));
		builder.set(
				"Latitude",
				latitude);
		builder.set(
				"Longitude",
				longitude);
		builder.set(
				"Location",
				location);

		return builder.buildFeature(featureId);
	}

}
