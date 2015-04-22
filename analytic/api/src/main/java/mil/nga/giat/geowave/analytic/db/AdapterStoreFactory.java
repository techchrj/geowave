package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.store.adapter.AdapterStore;

public interface AdapterStoreFactory
{
	public AdapterStore getAdapterStore(
			ConfigurationWrapper context )
			throws InstantiationException;
}
