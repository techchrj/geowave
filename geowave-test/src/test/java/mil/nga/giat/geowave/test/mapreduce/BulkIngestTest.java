package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.examples.ingest.bulk.BulkIngestMapper;
import mil.nga.giat.geowave.examples.ingest.bulk.GeonamesExportFileInputFormat;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class BulkIngestTest
{
	private static final Logger LOGGER = Logger.getLogger(BulkIngestTest.class);
	// private static final String TEST_RESOURCE_PACKAGE =
	// "mil/nga/giat/geowave/test/geonames";
	private static final String HDFS_INPUT_PATH = ""; // TODO set me!
	private static final String HDFS_OUTPUT_PATH = ""; // TODO set me!
	private static MiniAccumuloCluster accumulo;
	private static final String MINI_ACCUMULO_PASSWORD = "Ge0wave";

	@BeforeClass
	public static void setUp()
			throws IOException,
			InterruptedException {

		Logger.getRootLogger().setLevel(
				Level.INFO);

		accumulo = new MiniAccumuloCluster(
				new MiniAccumuloConfig(
						Files.createTempDir(),
						MINI_ACCUMULO_PASSWORD));

		LOGGER.info("Starting Mini Accumulo instance...");

		accumulo.start();

		LOGGER.info("Mini Accumulo instance is started.");

		// TODO stage example Geonames data to HDFS_INPUT_PATH
	}

	@Test
	public void testBulkIngestInputGeneration()
			throws Exception {

		System.out.println("Hello console");
		Assert.assertTrue(true);

		final String[] args = null; // TODO set args!!!
		ToolRunner.run(
				new BulkIngestJobRunner(),
				args);
	}

	@AfterClass
	public static void cleanUp()
			throws IOException,
			InterruptedException {

		LOGGER.info("Stopping Mini Accumulo instance...");

		accumulo.stop();

		LOGGER.info("Mini Accumulo instance is stopped.");
	}

	private static class BulkIngestJobRunner extends
			Configured implements
			Tool
	{

		@Override
		public int run(
				String[] args )
				throws Exception {

			final Configuration conf = getConf();
			final Job job = Job.getInstance(
					conf,
					"BulkIngestTestJob");
			job.setJarByClass(getClass());

			FileInputFormat.setInputPaths(
					job,
					new Path(
							HDFS_INPUT_PATH));
			FileOutputFormat.setOutputPath(
					job,
					new Path(
							HDFS_OUTPUT_PATH));

			job.setMapperClass(BulkIngestMapper.class);
			job.setReducerClass(Reducer.class); // (Identity Reducer)

			job.setInputFormatClass(GeonamesExportFileInputFormat.class);
			job.setOutputFormatClass(AccumuloFileOutputFormat.class);

			job.setMapOutputKeyClass(Key.class);
			job.setMapOutputValueClass(Value.class);
			job.setOutputKeyClass(Key.class);
			job.setOutputValueClass(Value.class);

			job.setNumReduceTasks(1);
			job.setSpeculativeExecution(false);

			return job.waitForCompletion(true) ? 0 : 1;
		}
	}

}
