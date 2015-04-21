package mil.nga.giat.geowave.analytics.sample.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.analytics.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.sample.RandomProbabilitySampleFn;
import mil.nga.giat.geowave.analytics.sample.SampleProbabilityFn;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rank objects using their distance to the closest centroid of a set of
 * centroids. The specific rank is determined by the probability of the point
 * meeting being a centroid, modeled in the implementation of
 * {@link SampleProbabilityFn}.
 * 
 * The farther the distance, the higher the rank.
 * 
 * @formatter:off Properties:
 * 
 *                "CentroidDistanceBasedSamplingRankFunction.KMeansConfig.data_store_configuration"
 *                - The class used to determine the prefix class name for te
 *                GeoWave Data Store parameters for a connection to collect the
 *                starting set of centroids. Defaults to
 *                {@link CentroidDistanceBasedSamplingRankFunction}.
 * 
 * 
 *                "CentroidDistanceBasedSamplingRankFunction.KMeansConfig.probability_function"
 *                - implementation of {@link SampleProbabilityFn}
 * 
 *                "CentroidDistanceBasedSamplingRankFunction.KMeansConfig.distance_function"
 *                - {@link DistanceFn}
 * 
 *                "CentroidDistanceBasedSamplingRankFunction.KMeansConfig.centroid_factory"
 *                - {@link AnalyticItemWrapperFactory} to wrap the centroid data
 *                with the appropriate centroid wrapper
 *                {@link AnalyticItemWrapper}
 * 
 * @ee CentroidManagerGeoWave
 * 
 * 
 * @formatter:on
 * 
 *               See {@link GeoWaveConfiguratorBase} for information for
 *               configuration GeoWave Data Store for consumption of starting
 *               set of centroids.
 * 
 * @param <T>
 *            The data type for the object being sampled
 */
public class CentroidDistanceBasedSamplingRankFunction<T> implements
		SamplingRankFunction<T>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(CentroidDistanceBasedSamplingRankFunction.class);

	private SampleProbabilityFn sampleProbabilityFn;
	private NestedGroupCentroidAssignment<T> nestedGroupCentroidAssigner;
	private final Map<String, Double> groupToConstant = new HashMap<String, Double>();
	protected AnalyticItemWrapperFactory<T> itemWrapperFactory;;

	public static void setParameters(
			final Configuration config,
			final PropertyManagement runTimeProperties ) {
		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);
		RunnerUtils.setParameter(
				config,
				CentroidDistanceBasedSamplingRankFunction.class,
				runTimeProperties,
				new ParameterEnum[] {
					SampleParameters.Sample.PROBABILITY_FUNCTION,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				});
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException {
		try {
			sampleProbabilityFn = context.getInstance(
					SampleParameters.Sample.PROBABILITY_FUNCTION,
					CentroidDistanceBasedSamplingRankFunction.class,
					SampleProbabilityFn.class,
					RandomProbabilitySampleFn.class);
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}

		try {
			itemWrapperFactory = context.getInstance(
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidDistanceBasedSamplingRankFunction.class,
					AnalyticItemWrapperFactory.class,
					SimpleFeatureItemWrapperFactory.class);

			itemWrapperFactory.initialize(context);
		}
		catch (final Exception e1) {

			throw new IOException(
					e1);
		}

		try {
			nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<T>(
					context);
		}
		catch (final Exception e1) {
			throw new IOException(
					e1);
		}

	}

	/**
	 * 
	 */
	@Override
	public double rank(
			final int sampleSize,
			final T value ) {
		final AnalyticItemWrapper<T> item = itemWrapperFactory.create(value);
		final List<AnalyticItemWrapper<T>> centroids = new ArrayList<AnalyticItemWrapper<T>>();
		double weight;
		try {
			weight = nestedGroupCentroidAssigner.findCentroidForLevel(
					item,
					new AssociationNotification<T>() {
						@Override
						public void notify(
								final CentroidPairing<T> pairing ) {
							try {
								centroids.addAll(nestedGroupCentroidAssigner.getCentroidsForGroup(pairing.getCentroid().getGroupID()));
							}
							catch (final IOException e) {
								throw new RuntimeException(
										e);
							}
						}
					});
		}
		catch (IOException e) {
			throw new RuntimeException(
					e);
		}
		return sampleProbabilityFn.getProbability(
				weight,
				getNormalizingConstant(
						centroids.get(
								0).getGroupID(),
						centroids),
				sampleSize);
	}

	private double getNormalizingConstant(
			final String groupID,
			final List<AnalyticItemWrapper<T>> centroids ) {

		if (!groupToConstant.containsKey(groupID)) {
			double constant = 0.0;
			for (final AnalyticItemWrapper<T> centroid : centroids) {
				constant += centroid.getCost();
			}
			groupToConstant.put(
					groupID,
					constant);
		}
		return groupToConstant.get(
				groupID).doubleValue();

	}
}
