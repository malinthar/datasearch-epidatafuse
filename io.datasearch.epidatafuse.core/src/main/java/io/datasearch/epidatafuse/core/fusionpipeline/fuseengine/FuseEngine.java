package io.datasearch.epidatafuse.core.fusionpipeline.fuseengine;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.PipelineDataStore;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.AggregationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.GranularityRelationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.dataframe.DataFrame;
import io.datasearch.epidatafuse.core.fusionpipeline.model.datamodel.SpatioTemporallyAggregatedCollection;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.GranularityMap;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.TemporalGranularityMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

/**
 * For data fusion.
 */
public class FuseEngine {

    private static final Logger logger = LoggerFactory.getLogger(FuseEngine.class);
    //aggregating
    private DataFrameBuilder dataFrameBuilder;
    //granularityConvertor
    private GranularityConvertor granularityConvertor;
    //granularityRelationMapper
    private GranularityRelationMapper granularityRelationMapper;

    private PipelineDataStore dataStore;

    private Scheduler scheduler;

    private Map<String, GranularityRelationConfig> granularityRelationConfigs;
    private Map<String, AggregationConfig> aggregationConfigs;

    private Map<String, GranularityMap> granularityRelationMaps;
    private long fusionFrequency;
    private String fusionFQUnit;
    private String fusionFQMultiplier;
    private Timer timer = new Timer();
    private String pipelineName;
    private String initTimestamp;

    public FuseEngine(PipelineDataStore dataStore, String pipelineName,
                      Map<String, GranularityRelationConfig> granularityRelationConfigs,
                      Map<String, AggregationConfig> aggregationConfigs) {
        this.dataStore = dataStore;
        this.granularityRelationMapper = new GranularityRelationMapper(this.dataStore);
        this.granularityConvertor = new GranularityConvertor(this.dataStore);
        this.granularityRelationConfigs = granularityRelationConfigs;
        this.aggregationConfigs = aggregationConfigs;
        this.dataFrameBuilder = new DataFrameBuilder();
        this.scheduler = new Scheduler();
        this.pipelineName = pipelineName;
        scheduler.setFuseEngine(this);
    }

    public Map<String, GranularityMap> invokeGranularityMappingProcess(
            Map<String, GranularityRelationConfig> granularityRelationConfigs) {

        Map<String, GranularityMap> granularityMaps = this.buildGranularityMap(granularityRelationConfigs);
        this.granularityRelationMaps = granularityMaps;
        return granularityMaps;
    }


    public Map<String, GranularityMap> buildGranularityMap(
            Map<String, GranularityRelationConfig> granularityRelationConfigs) {

        Map<String, GranularityMap> granularityMaps = new HashMap<>();
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

    public void invokeAggregationProcess() {

        String dtg = LocalDateTime.now().toString();

        DataFrame dataFrame = new DataFrame(dtg);

        if ((granularityRelationMaps != null) && granularityRelationMaps.size() > 0) {
            this.granularityRelationMaps.forEach((String featureTypeName, GranularityMap granularityMap) -> {
                AggregationConfig aggregationConfig = this.aggregationConfigs.get(featureTypeName);
                try {
                    SpatioTemporallyAggregatedCollection spatioTemporallyAggregatedCollection =
                            this.aggregate(granularityMap, aggregationConfig);
                    dataFrame.addAggregatedFeatureType(spatioTemporallyAggregatedCollection);
                } catch (IOException e) {
                    e.getMessage();
                }
            });

            ArrayList<String> csvRecords = dataFrame.createCSVRecords();

            String fileName =
                    "finalDataFrame_" + dataFrame.getSpatialGranularity() + "_" + dataFrame.getTemporalGranularity() +
                            "_" + dtg;
            this.dataFrameBuilder.writeToCSV(csvRecords, fileName, pipelineName);

        } else {
            logger.info("Cannot aggregate. granularity map is empty");
        }
    }

    public SpatioTemporallyAggregatedCollection aggregate(GranularityMap granularityMap,
                                                          AggregationConfig aggregationConfig) throws IOException {
        SpatioTemporallyAggregatedCollection spatioTemporallyAggregatedCollection =
                this.granularityConvertor.aggregate(granularityMap, aggregationConfig);

        //this.dataFrameBuilder.buildDataFrame(spatioTemporallyAggregatedCollection);
        return spatioTemporallyAggregatedCollection;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void scheduleTasks() {
        this.timer.schedule(scheduler, 0, this.fusionFrequency);
    }

    public void setFusionFrequency(long fusionFrequency) {
        this.fusionFrequency = fusionFrequency;
    }

    public void setFusionFQUnit(String fusionFQUnit) {
        this.fusionFQUnit = fusionFQUnit;
    }

    public void setFusionFQMultiplier(String fusionFQMultiplier) {
        this.fusionFQMultiplier = fusionFQMultiplier;
    }

    public long getFusionFrequency() {
        return fusionFrequency;
    }

    public String getFusionFQUnit() {
        return fusionFQUnit;
    }

    public String getFusionFQMultiplier() {
        return fusionFQMultiplier;
    }

    public DataFrameBuilder getDataFrameBuilder() {
        return dataFrameBuilder;
    }

    public void setFusionInitTimestamp(String initTimestamp) {
        this.initTimestamp = initTimestamp;
        this.granularityConvertor.setFusionInitTimestamp(initTimestamp);
    }
}

