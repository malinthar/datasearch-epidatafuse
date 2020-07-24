package io.datasearch.denguestore.ingestion;
import io.datasearch.denguestore.data.DiseaseData;
import io.datasearch.denguestore.data.FeatureData;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FeatureSource;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.DefaultTransaction;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLine;
import org.geotools.data.DataStore;
import io.datasearch.diseasedata.store.schema.SimpleFeatureTypeSchema;

public class DataIngester{
	  private static final Logger logger = LoggerFactory.getLogger(DataIngester.class);

    public List<SimpleFeature> createFetures(Map<String, Object> parameters,Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas){
   		DiseaseData fd = new FeatureData(parameters,simpleFeatureTypeSchemas);
   		return fd.getFeatureData();
    }

    public void insertData(DataStore datastore,Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) throws Exception{
		
      Map<String, Object> configurations = ConfigurationLoader.getIngestConfigurations();
      String featureName = (String)configurations.get("feature_name");
      DataStore dataStore = datastore;
      SimpleFeatureSource featureSource = dataStore.getFeatureSource(featureName);
      SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
      logger.info("featureStore from datastore "+featureSource);
      List<SimpleFeature> features = createFetures(configurations,simpleFeatureTypeSchemas);

     	for(SimpleFeature feature:features){
       		SimpleFeatureCollection collection = DataUtilities.collection(feature);
       		featureStore.addFeatures(collection);	
     	}
     
      logger.info("Add "+features.size()+" new features");
    }
}