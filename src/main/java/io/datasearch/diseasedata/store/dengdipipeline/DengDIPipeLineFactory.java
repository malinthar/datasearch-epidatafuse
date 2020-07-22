package io.datasearch.diseasedata.store.dengdipipeline;

import io.datasearch.diseasedata.store.data.FeatureData;
import io.datasearch.diseasedata.store.schema.SchemaBuilder;
import io.datasearch.diseasedata.store.util.CommandLineDataStore;
import io.datasearch.diseasedata.store.util.FeatureConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Factory for creating pipelines.
 */
public class DengDIPipeLineFactory {

    private static final Logger logger = LoggerFactory.getLogger(DengDIPipeLineFactory.class);
    private static SchemaBuilder schemaBuilder = new SchemaBuilder();


    public DengDIPipeLine createDengDIPipeLine(String[] args) {
        try {

            SchemaBuilder schemaBuilder =  new SchemaBuilder(params, false);
            //Generate data from config
            this.data = new FeatureData(FeatureConfigurator.getFeatureConfiguration());
            cleanup = command.hasOption("cleanup"); //todo: revisit
        } catch (ParseException e) {
            logger.error(e.getMessage());
        }
        return new DengDIPipeLine(params);
    }


    public DataStore findDataStore(Map<String, String> params) throws IOException {
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not find or create a data store with provided parameters");
        }
        logger.info("Successfully created or found datastore");
        return datastore;
    }

}
