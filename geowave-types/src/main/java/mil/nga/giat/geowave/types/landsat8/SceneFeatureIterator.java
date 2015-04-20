package mil.nga.giat.geowave.types.landsat8;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.NoSuchElementException;

import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class SceneFeatureIterator implements
		FeatureIterator<SimpleFeature>
{
	private static final String SCENES_GZ_URL = "http://landsat-pds.s3.amazonaws.com/scene_list.gz";

	private final String SCENES_DIR = "scenes";
	private final String COMPRESSED_FILE_NAME = "scene_list.gz";
	private final String CSV_FILE_NAME = "scene_list";
	private final boolean onlyScenesSinceLastRun;
	private final Filter cqlFilter;
	private final WRS2GeometryStore geometryStore;

	public SceneFeatureIterator(
			final boolean onlyScenesSinceLastRun,
			final Filter cqlFilter,
			final String workspaceDir )
			throws MalformedURLException,
			IOException {
		this.onlyScenesSinceLastRun = onlyScenesSinceLastRun;
		this.cqlFilter = cqlFilter;
		init(new File(
				workspaceDir,
				SCENES_DIR));
		geometryStore = new WRS2GeometryStore(
				workspaceDir);
	}

	private void init(
			final File scenesDir ) {
		final File compressedFile = new File(
				scenesDir,
				COMPRESSED_FILE_NAME);
		if (!compressedFile.exists()) {
			// if it doesn't exist, download it and start from the beginning
		}
		else {
			// check the size of the download and if its the same, just use the
			// current file

			if (onlyScenesSinceLastRun) {
				// seek the number of lines of the existing file
			}
		}
	}

	@Override
	public void close() {

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public SimpleFeature next()
			throws NoSuchElementException {
		// TODO Auto-generated method stub
		return null;
	}

}
