package io.datasearch.diseasedata.store.schema;

import io.datasearch.diseasedata.store.data.DiseaseData;
import io.datasearch.diseasedata.store.util.CommandLineDataStore;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SchemaBuilder class for creating new feature types.
 */
public class SchemaBuilder implements Runnable {
    private final Map<String, String> params;
    private final DiseaseData data;
    private final boolean cleanup;
    private final boolean readOnly;
    private static final Logger logger = LoggerFactory.getLogger(SchemaBuilder.class);

    public SchemaBuilder(String[] args, DataAccessFactory.Param[] parameters, DiseaseData data, boolean readOnly)
            throws ParseException {
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        cleanup = command.hasOption("cleanup");
        this.data = data;
        this.readOnly = readOnly;
        initializeFromOptions(command);
    }

    public Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        }
        return options;
    }

    public void initializeFromOptions(CommandLine command) {
    }

    @Override
    public void run() {
        DataStore datastore = null;
        try {
            datastore = createDataStore(params);

            if (readOnly) {
                ensureSchema(datastore, data);
            } else {
                SimpleFeatureType sft = getSimpleFeatureType(data);
                createSchema(datastore, sft);
                List<SimpleFeature> features = getTestFeatures(data);
                writeFeatures(datastore, sft, features);
            }

            List<Query> queries = getTestQueries(data);

            queryFeatures(datastore, queries);
        } catch (Exception e) {

            logger.error(e.getMessage());
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName(), cleanup);
        }
        logger.info("Done");
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        logger.info("Loading datastore");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        logger.info("created data");
        return datastore;
    }

    public void ensureSchema(DataStore datastore, DiseaseData data) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(data.getTypeName());
        if (sft == null) {
            throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist. "
                    + "Please run the associated QuickStart to generate the test data.");
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

    public List<SimpleFeature> getTestFeatures(DiseaseData data) {
        logger.info("Generating test data");
        List<SimpleFeature> features = data.getTestData();
        logger.info("");
        return features;
    }

    public List<Query> getTestQueries(DiseaseData data) {
        return data.getTestQueries();
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features)
            throws IOException {
        if (features.size() > 0) {
            logger.info("Writing test data");
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore
                    .getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need
                    // to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation
                    // class
                    // and add the USE_PROVIDED_FID hint to the user data
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
            logger.info("Wrote " + features.size() + " features");
            logger.info("");
        }
    }

    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        for (Query query : queries) {
            logger.info("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                logger.info("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                logger.info("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            // submit the query, and get back an iterator over matching features
            // use try-with-resources to ensure the reader is closed
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(query,
                    Transaction.AUTO_COMMIT)) {
                // loop through all results, only print out the first 10
                int n = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        logger.info(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                    } else if (n == 10) {
                        logger.info("...");
                    }
                }
                logger.info("");
                logger.info("Returned " + n + " total features");
                logger.info("");
            }
        }
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
