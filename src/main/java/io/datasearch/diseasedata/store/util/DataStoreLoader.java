package io.datasearch.diseasedata.store.util;

import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLineFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Finds the Hbase Data store for given params.
 */
public class DataStoreLoader {
    //Get DataStoreAccess parameters info
    private static final DataAccessFactory.Param[] parameters =
            new HBaseDataStoreFactory().getParametersInfo();
    private static final Logger logger = LoggerFactory.getLogger(DengDIPipeLineFactory.class);

    public static DataStore findDataStore(String[] args) {
        try {
            //generate the set of parameters from parameter info
            Options options = createOptions(parameters);
            CommandLine command = CommandLineDataStore.parseArgs(DataStoreLoader.class, options, args);
            Map<String, String> datastoreParams = CommandLineDataStore.getDataStoreParams(command, options);
            DataStore datastore = DataStoreFinder.getDataStore(datastoreParams);
            return datastore;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null; //todo: revisit
        }
    }

    public static Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        return options;
    }
}
