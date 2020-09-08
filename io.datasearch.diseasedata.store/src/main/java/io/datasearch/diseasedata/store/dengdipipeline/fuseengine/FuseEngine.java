package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;


import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.GranularityRelationConfig;
import org.geotools.data.DataStore;

import java.util.HashMap;

/**
 * For data fusion.
 */
public class FuseEngine {
    //aggregating
    private DataFrameBuilder dataFrameBuilder;
    //granularityConvertor
    private GranularityConvertor granularityConvertor;
    //granularityRelationMapper
    private GranularityRelationMapper granularityRelationMapper;

    private DataStore dataStore;

    private HashMap<String, GranularityRelationConfig> spatialGranularityConfigs =
            new HashMap<String, GranularityRelationConfig>();

    public FuseEngine(DataStore dataStore) {
        this.dataStore = dataStore;
        this.granularityRelationMapper = new GranularityRelationMapper(this.dataStore);
    }

    public GranularityRelationMapper getGranularityRelationMapper() {
        return this.granularityRelationMapper;
    }

    public void setTargetGranularities() {
        this.granularityRelationMapper.setTargetGranularities("moh", "week");
    }

    public void setSpatialGranularityConfigs() {
        //read from the file and set the hashmap
        String featureTypeName = "precipitation";
        GranularityRelationConfig spatialConfig = new GranularityRelationConfig(featureTypeName,
                "weatherstations", "nearest");

        spatialConfig.setCustomAttributes("neighbors", "3");
        spatialConfig.setCustomAttributes("maxDistance", "100000");

        spatialGranularityConfigs.put(featureTypeName, spatialConfig);
    }

    public HashMap<String, GranularityRelationConfig> getSpatialGranularityConfigs() {
        return spatialGranularityConfigs;
    }

//    public GranularityConvertor getGranularityConvertor() {
//        if (this.granularityConvertor == null) {
//            this.granularityConvertor = new GranularityConvertor(th);
//        }
//        return this.granularityConvertor;
//    }
}

