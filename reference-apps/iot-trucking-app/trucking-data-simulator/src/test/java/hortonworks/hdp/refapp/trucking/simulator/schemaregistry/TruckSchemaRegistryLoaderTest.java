package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

import org.junit.Test;

public class TruckSchemaRegistryLoaderTest {

	private static final String SCHEMA_REGISTRY_URL = "http://hdf-ref-app-web0.field.hortonworks.com:9099/api/v1";

	@Test
	public void loadSchemaRegistryWithTruckSchemas() {
		TruckSchemaRegistryLoader registryLoader = new TruckSchemaRegistryLoader(SCHEMA_REGISTRY_URL);
		registryLoader.loadSchemaRegistry();
	}
}
