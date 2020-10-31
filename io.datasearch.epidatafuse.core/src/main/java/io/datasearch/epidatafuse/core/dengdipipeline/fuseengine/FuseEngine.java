package io.datasearch.epidatafuse.core.dengdipipeline.fuseengine;


import io.datasearch.epidatafuse.core.dengdipipeline.models.configmodels.AggregationConfig;
import io.datasearch.epidatafuse.core.dengdipipeline.models.configmodels.GranularityRelationConfig;
import io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap.GranularityMap;
import io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap.SpatialGranularityRelationMap;
import io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap.TemporalGranularityMap;

import org.geotools.data.DataStore;

import java.io.IOException;
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
        this.granularityConvertor = new GranularityConvertor(this.dataStore);
    }

    public GranularityRelationMapper getGranularityRelationMapper() {
        return this.granularityRelationMapper;
    }

    public HashMap<String, GranularityRelationConfig> getSpatialGranularityConfigs() {
        return spatialGranularityConfigs;
    }

    public HashMap<String, SpatialGranularityRelationMap> buildSpatialGranularityMap(
            HashMap<String, GranularityRelationConfig> granularityRelationConfigs) {

        HashMap<String, SpatialGranularityRelationMap> relationMaps =
                new HashMap<String, SpatialGranularityRelationMap>();

        granularityRelationConfigs.forEach((featureType, config) -> {
            SpatialGranularityRelationMap spatialGranularityRelationMap =
                    granularityRelationMapper.buildSpatialGranularityMap(config);
            relationMaps.put(featureType, spatialGranularityRelationMap);
        });

        return relationMaps;
    }


    public HashMap<String, GranularityMap> buildGranularityMap(
            HashMap<String, GranularityRelationConfig> granularityRelationConfigs) {

        HashMap<String, GranularityMap> granularityMaps = new HashMap<String, GranularityMap>();

        granularityRelationConfigs.forEach((featureType, granularityRelationConfig) -> {

            SpatialGranularityRelationMap spatialMap =
                    this.granularityRelationMapper.buildSpatialGranularityMap(granularityRelationConfig);
            TemporalGranularityMap temporalMap =
                    this.granularityRelationMapper.buildTemporalMap(granularityRelationConfig);

            String baseSpatialGranularity = granularityRelationConfig.getSpatialGranularity();
            String baseTemporalGranularity = granularityRelationConfig.getTemporalGranularity();
            String targetSpatialGranularity = granularityRelationConfig.getTargetSpatialGranularity();
            String targetTemporalGranularity = granularityRelationConfig.getTargetTemporalGranularity();

            GranularityMap granularityMap =
                    new GranularityMap(featureType, spatialMap, temporalMap, baseSpatialGranularity,
                            baseTemporalGranularity, targetSpatialGranularity,
                            targetTemporalGranularity);

            granularityMaps.put(featureType, granularityMap);
        });

        return granularityMaps;
    }

    public void aggregate(GranularityMap granularityMap, AggregationConfig aggregationConfig) throws IOException {
        this.granularityConvertor.aggregate(granularityMap, aggregationConfig);
    }
}

