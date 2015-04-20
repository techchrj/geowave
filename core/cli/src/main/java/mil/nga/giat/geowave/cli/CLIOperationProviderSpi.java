package mil.nga.giat.geowave.cli;

public interface CLIOperationProviderSpi
{
	public CLIOperation[] getOperations();

	public CLIOperationCategory getCategory();
}
