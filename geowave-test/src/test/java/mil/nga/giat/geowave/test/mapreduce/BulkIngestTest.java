package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.examples.ingest.bulk.BulkIngestMapper;
import mil.nga.giat.geowave.examples.ingest.bulk.GeonamesExportFileInputFormat;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class BulkIngestTest
{
	private static final Logger LOGGER = Logger.getLogger(BulkIngestTest.class);
	private static final String TEST_DATA_LOCATION = "src/test/resources/mil/nga/giat/geowave/test/geonames";
	private static final String OUTPUT_PATH = "target/tmp_bulkIngestTest";

	// private static MiniAccumuloCluster accumulo;
	// private static final String MINI_ACCUMULO_PASSWORD = "Ge0wave";

	@BeforeClass
	public static void setUp()
			throws IOException,
			InterruptedException {

		Logger.getRootLogger().setLevel(
				Level.INFO);

		// accumulo = new MiniAccumuloCluster(
		// new MiniAccumuloConfig(
		// Files.createTempDir(),
		// MINI_ACCUMULO_PASSWORD));
		//
		// LOGGER.info("Starting Mini Accumulo instance...");
		//
		// accumulo.start();
		//
		// LOGGER.info("Mini Accumulo instance is started.");
	}

	@Test
	public void testBulkIngestInputGeneration()
			throws Exception {

		LOGGER.info("Running Bulk Ingest Input Generation MapReduce job...");

		final BulkIngestJobRunner jobRunner = new BulkIngestJobRunner();

		ToolRunner.run(
				jobRunner,
				null);

		// TODO verify sequence file contents

		// Configuration conf = jobRunner.getConf();
		// FileSystem fs = FileSystem.get(
		// URI.create("part-r-00000"),
		// conf);
		// Path path = new Path(
		// OUTPUT_PATH);
		//
		// SequenceFile.Reader reader = null;
		// try {
		// reader = new SequenceFile.Reader(
		// fs,
		// path,
		// conf);
		// // reader = new SequenceFile.Reader(
		// // conf,
		// // SequenceFile.Reader.file(path));
		// Writable key = (Writable) ReflectionUtils.newInstance(
		// reader.getKeyClass(),
		// conf);
		// Writable value = (Writable) ReflectionUtils.newInstance(
		// reader.getValueClass(),
		// conf);
		// long position = reader.getPosition();
		// while (reader.next(
		// key,
		// value)) {
		// String syncSeen = reader.syncSeen() ? "*" : "";
		// System.out.printf(
		// "[%s%s]\t%s\t%s\n",
		// position,
		// syncSeen,
		// key,
		// value);
		// position = reader.getPosition(); // beginning of next record
		// }
		// }
		// finally {
		// IOUtils.closeStream(reader);
		// }

		Assert.assertTrue(true);
	}

	@AfterClass
	public static void cleanUp()
			throws IOException,
			InterruptedException {

		// LOGGER.info("Stopping Mini Accumulo instance...");
		//
		// accumulo.stop();
		//
		// LOGGER.info("Mini Accumulo instance is stopped.");
	}

	private static class BulkIngestJobRunner extends
			Configured implements
			Tool
	{
		private static final String JOB_NAME = "BulkIngestTestJob";

		@Override
		public int run(
				String[] args )
				throws Exception {

			final Configuration conf = getConf();

			// FIXME (currently running locally on Windows)
			conf.set(
					"fs.defaultFS",
					"file:///");

			final Job job = Job.getInstance(
					conf,
					JOB_NAME);
			job.setJarByClass(getClass());

			FileInputFormat.setInputPaths(
					job,
					new Path(
							TEST_DATA_LOCATION));
			FileOutputFormat.setOutputPath(
					job,
					cleanPathForReuse(
							conf,
							OUTPUT_PATH));

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

		private Path cleanPathForReuse(
				Configuration conf,
				String pathString )
				throws IOException {

			final FileSystem fs = FileSystem.get(conf);
			final Path path = new Path(
					pathString);

			if (fs.exists(path)) {
				LOGGER.info("Deleting '" + pathString + "' for reuse.");
				fs.delete(
						path,
						true);
			}

			return path;
		}
	}

}
