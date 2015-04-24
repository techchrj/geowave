package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;

import mil.nga.giat.geowave.store.GeometryUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * GeoNames provides exports by country (see <a
 * href="http://download.geonames.org/export/dump/"
 * >http://download.geonames.org/export/dump/</a>). These files contain one
 * tab-delimited entry per line.<br />
 * <br />
 * This class primarily delegates to {@link LineRecordReader}. The
 * <code>getCurrentValue()</code> method handles the conversion of the line (
 * <code>Text</code>) value from the Geonames export file into a
 * {@link SimpleFeature}
 */
public class GeonamesRecordReader extends
		RecordReader<LongWritable, SimpleFeature>
{
	private final LineRecordReader lineRecordReader = new LineRecordReader();
	private final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
			GeonamesSimpleFeatureType.getInstance());
	private String[] geonamesEntryTokens;
	private String geonameId;
	private double longitude;
	private double latitude;
	private String location;

	@Override
	public void initialize(
			InputSplit split,
			TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		lineRecordReader.initialize(
				split,
				context);
	}

	@Override
	public boolean nextKeyValue()
			throws IOException,
			InterruptedException {
		return lineRecordReader.nextKeyValue();
	}

	@Override
	public LongWritable getCurrentKey()
			throws IOException,
			InterruptedException {
		return lineRecordReader.getCurrentKey();
	}

	@Override
	public SimpleFeature getCurrentValue()
			throws IOException,
			InterruptedException {
		return parseGeonamesValue(lineRecordReader.getCurrentValue());
	}

	@Override
	public float getProgress()
			throws IOException,
			InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void close()
			throws IOException {
		lineRecordReader.close();
	}

	private SimpleFeature parseGeonamesValue(
			Text value ) {

		geonamesEntryTokens = value.toString().split(
				"\\t"); // Exported Geonames entries are tab-delimited

		geonameId = geonamesEntryTokens[0];
		location = geonamesEntryTokens[1];
		latitude = Double.parseDouble(geonamesEntryTokens[4]);
		longitude = Double.parseDouble(geonamesEntryTokens[5]);

		return buildSimpleFeature(
				geonameId,
				longitude,
				latitude,
				location);
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
