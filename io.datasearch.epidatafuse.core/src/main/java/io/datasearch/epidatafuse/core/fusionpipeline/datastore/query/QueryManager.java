package io.datasearch.epidatafuse.core.fusionpipeline.datastore.query;


import io.datasearch.epidatafuse.core.DiseaseDataStoreService;
import io.datasearch.epidatafuse.core.fusionpipeline.FusionPipeline;
import io.datasearch.epidatafuse.core.util.ConfigurationLoader;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Query Manager Class.
 */
public class QueryManager {

    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStoreService.class);

    public List<QueryObject> readQueriesFromFile() throws Exception {

        List<QueryObject> queryObjects = new ArrayList<QueryObject>();
        Map<String, Object> yml = ConfigurationLoader.getQueryConfigurations();

        List<Map<String, String>> ymlObject = (List<Map<String, String>>) yml.get("queries");
        for (Map<String, String> i : ymlObject) {
            Query query = new Query(i.get("schema"), ECQL.toFilter(i.get("query")));
            QueryObject queryObject = new QueryObject(i.get("catalog"), query, i.get("schema"));

            queryObjects.add(queryObject);
        }
        return queryObjects;

    }

    public DataStore getDataStore(String hbaseCatalog) throws Exception {
        Map<String, Serializable> parameters = new HashMap<>();
        parameters.put("hbase.catalog", hbaseCatalog);
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);
        return dataStore;
    }

    public ArrayList<SimpleFeature> readFeatureCollection(FeatureReader reader) throws Exception {
        ArrayList<SimpleFeature> featureList = new ArrayList<SimpleFeature>();
        int n = 0;
        while (reader.hasNext()) {
            SimpleFeature feature = (SimpleFeature) reader.next();
            featureList.add(feature);
        }
        return featureList;
    }

    public void logFeatures(FeatureReader reader) throws Exception {
        int n = 0;
        while (reader.hasNext()) {
            SimpleFeature feature = (SimpleFeature) reader.next();
            if (n++ < 1000) {
                // use geotools data utilities to get a printable string
                logger.info(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
            } else if (n == 1000) {
                logger.info("...");
            }
        }
        logger.info("");
        logger.info("Returned " + n + " total features");
        logger.info("");
    }


    public void runQueries(FusionPipeline dengDIPipeLine) throws Exception {

        List<QueryObject> queries = this.readQueriesFromFile();

        for (QueryObject obj : queries) {
            DataStore dataStore = dengDIPipeLine.getDataStore();
            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    dataStore.getFeatureReader(obj.query, Transaction.AUTO_COMMIT);
            this.logFeatures(reader);
        }

    }

    public ArrayList<SimpleFeature> getFeatures(DataStore dataStore, QueryObject query) throws Exception {
        FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                dataStore.getFeatureReader(query.query, Transaction.AUTO_COMMIT);
        ArrayList<SimpleFeature> featureList = this.readFeatureCollection(reader);
        return featureList;
    }

}
