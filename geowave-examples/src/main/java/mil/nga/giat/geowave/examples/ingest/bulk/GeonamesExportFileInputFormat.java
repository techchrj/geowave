package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.opengis.feature.simple.SimpleFeature;

/**
 * GeoNames provides exports by country (see <a
 * href="http://download.geonames.org/export/dump/"
 * >http://download.geonames.org/export/dump/</a>). These files contain one
 * tab-delimited entry per line.<br />
 * <br />
 * This class extends {@link FileInputFormat} to provide a
 * {@link LineRecordReader} to feed each GeoNames entry to {@link Mapper#map} as
 * the <code>value</code> parameter.
 */
public class GeonamesExportFileInputFormat extends
		FileInputFormat<LongWritable, SimpleFeature>
{
	@Override
	public RecordReader<LongWritable, SimpleFeature> createRecordReader(
			InputSplit split,
			TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		return new GeonamesRecordReader();
	}
}
