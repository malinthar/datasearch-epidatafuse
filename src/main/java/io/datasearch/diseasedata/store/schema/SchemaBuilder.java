package io.datasearch.diseasedata.store.schema;

import io.datasearch.diseasedata.store.data.DiseaseData;
import io.datasearch.diseasedata.store.data.FeatureData;
import io.datasearch.diseasedata.store.util.CommandLineDataStore;
import io.datasearch.diseasedata.store.util.FeatureConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureStore;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * SchemaBuilder class for creating new feature types.
 */
public class SchemaBuilder implements Runnable {

    private final DiseaseData data;
    private final boolean cleanup;
    private final boolean readOnly;
    private static final Logger logger = LoggerFactory.getLogger(SchemaBuilder.class);

    public void createSchema(boolean readOnly) throws ParseException {
        this.run();
    }

    /**
     * Builds the schema (SimpleFeatureType)
     */
    @Override
    public void run() {
        DataStore datastore = null;
        try {
            ensureSchema(datastore, data.getSimpleFeatureType().getTypeName());
            SimpleFeatureType sft = getSimpleFeatureType(data);
            createSchema(datastore, sft);

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName(), cleanup);
        }
        logger.info("Done");
    }

    public void ensureSchema(DataStore datastore, String simpleFeatureTypeName) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(simpleFeatureTypeName);
        if (sft == null) {
            throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist.");
        }
    }

    public SimpleFeatureType getSimpleFeatureType(DiseaseData data) {
        return data.getSimpleFeatureType();
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        logger.info("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        logger.info("");
    }

    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    logger.info("Cleaning up test data");
                    if (datastore instanceof GeoMesaDataStore) {
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                        datastore.removeSchema(typeName);
                    }
                }
            } catch (Exception e) {
                logger.error("Exception cleaning up test data: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }
}
