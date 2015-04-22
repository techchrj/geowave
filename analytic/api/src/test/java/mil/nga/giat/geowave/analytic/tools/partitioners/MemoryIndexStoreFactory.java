package mil.nga.giat.geowave.analytic.tools.partitioners;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.db.IndexStoreFactory;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.index.MemoryIndexStore;

public class MemoryIndexStoreFactory implements
		IndexStoreFactory
{

	final static MemoryIndexStore IndexStoreInstance = new MemoryIndexStore(
			new Index[0]);

	@Override
	public IndexStore getIndexStore(
			ConfigurationWrapper context )
			throws InstantiationException {
		return IndexStoreInstance;
	}

}
