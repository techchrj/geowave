package mil.nga.giat.geowave.examples.ingest.bulk;

import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class BulkIngestInputGeneratorJob extends
		GeoWaveAnalyticJobRunner
{

	@Override
	public void configure(
			Job job )
			throws Exception {

		job.setInputFormatClass(GeonamesExportFileInputFormat.class);
		job.setMapperClass(BulkIngestMapper.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);

		job.setOutputFormatClass(AccumuloFileOutputFormat.class);
		job.setReducerClass(Reducer.class); // Identity Reducer
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);

		// TODO - what else?
	}

}
