package mil.nga.giat.geowave.examples.ingest.bulk;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkIngestInputGeneratorJob extends
		Configured implements
		Tool
{

	@Override
	public int run(
			String[] args )
			throws Exception {

		Job job = Job.getInstance(
				getConf(),
				"BulkIngestInputGeneratorJob");
		// Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());

		// TODO
		// FileInputFormat.setInputPaths(job, inputPaths);
		// FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(BulkIngestMapper.class);
		job.setReducerClass(Reducer.class); // Identity Reducer

		job.setInputFormatClass(GeonamesExportFileInputFormat.class);
		job.setOutputFormatClass(AccumuloFileOutputFormat.class);

		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);

		// TODO - what else?

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(
			String[] args ) {
		int result;
		try {
			result = ToolRunner.run(
					new Configuration(),
					new BulkIngestInputGeneratorJob(),
					args);
			System.exit(result);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
