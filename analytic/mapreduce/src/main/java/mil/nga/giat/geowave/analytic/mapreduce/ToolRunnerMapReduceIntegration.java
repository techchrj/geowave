package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ToolRunnerMapReduceIntegration implements
		MapReduceIntegration
{

	@Override
	public Job getJob(
			Tool tool )
			throws IOException {
		return new Job(
				tool.getConf());
	}

	@Override
	public int submit(
			Configuration configuration,
			PropertyManagement runTimeProperties,
			GeoWaveAnalyticJobRunner tool )
			throws Exception {
		return ToolRunner.run(
				configuration,
				tool,
				runTimeProperties.toGeoWaveRunnerArguments());
	}

	@Override
	public boolean waitForCompletion(
			Job job )
			throws ClassNotFoundException,
			InterruptedException,
			Exception {
		return job.waitForCompletion(true);
	}

}
