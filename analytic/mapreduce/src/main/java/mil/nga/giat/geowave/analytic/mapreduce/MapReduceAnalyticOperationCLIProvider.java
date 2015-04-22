package mil.nga.giat.geowave.analytic.mapreduce;

import mil.nga.giat.geowave.analytic.AnalyticCLIOperationDriver;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelJumpKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNJobRunner;
import mil.nga.giat.geowave.cli.CLIOperation;
import mil.nga.giat.geowave.cli.CLIOperationCategory;
import mil.nga.giat.geowave.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.ingest.IngestOperationCategory;

public class MapReduceAnalyticOperationCLIProvider implements
		CLIOperationProviderSpi
{
	/**
	 * This identifies the set of operations supported and which driver to
	 * execute based on the operation selected.
	 */
	private static final CLIOperation[] MR_ANALYTIC_OPERATIONS = new CLIOperation[] {
		new CLIOperation(
				"kmeans-parallel",
				"KMeans Parallel Clustering",
				new AnalyticCLIOperationDriver(
						new MultiLevelKMeansClusteringJobRunner())),
		new CLIOperation(
				"nn",
				"Nearest Neighbors",
				new AnalyticCLIOperationDriver(
						new NNJobRunner())),
		new CLIOperation(
				"kmeans-jump",
				"KMeans Clustering using Jump Method",
				new AnalyticCLIOperationDriver(
						new MultiLevelJumpKMeansClusteringJobRunner()))
	};

	private static final CLIOperationCategory CATEGORY = new IngestOperationCategory();

	@Override
	public CLIOperation[] getOperations() {
		return MR_ANALYTIC_OPERATIONS;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return CATEGORY;
	}

}
