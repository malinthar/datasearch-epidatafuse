package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import io.datasearch.diseasedata.store.dengdipipeline.models.SpatialGranularityRelationMap;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.GranularityRelationConfig;
import io.datasearch.diseasedata.store.dengdipipeline.models.granularitymappingmethods.NearestMapper;
import io.datasearch.diseasedata.store.query.QueryManager;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * For granularity Mapping.
 */
public class GranularityRelationMapper {

    private DataStore dataStore;
    private QueryManager queryManager;
    private static final Logger logger = LoggerFactory.getLogger(GranularityRelationMapper.class);

    private String targetSpatialGranularity;
    private String targetTemporalGranularity;
    private ArrayList<Map<String, Object>> spatioTemporalGranularityConfigs;

    private SimpleFeatureCollection targetSpatialGranules;

    public GranularityRelationMapper(DataStore dataStore) {
        this.dataStore = dataStore;
        this.queryManager = new QueryManager();
    }

    public void setTargetGranularities(String targetSpatial, String targetTemporal) {
        this.targetSpatialGranularity = targetSpatial;
        this.targetTemporalGranularity = targetTemporal;
    }

    public SpatialGranularityRelationMap buildSpatialGranularityMaps(GranularityRelationConfig config) {
        //String featureType = config.getFeatureTypeName();
        String spatialGranularity = config.getSpatialGranularity();
        String relationMappingMethod = config.getRelationMappingMethod();
        SpatialGranularityRelationMap spatialMap;

        if (this.targetSpatialGranules == null) {
            this.targetSpatialGranules = this.getGranuleSet(this.targetSpatialGranularity);
        }

        SimpleFeatureCollection baseSpatialGranuleSet = this.getGranuleSet(spatialGranularity);

        switch (relationMappingMethod) {
            case "nearest":
                spatialMap = NearestMapper.buildNearestMap(this.targetSpatialGranules,
                        baseSpatialGranuleSet, 3, 1000000);
                break;

            default:
                spatialMap = new SpatialGranularityRelationMap();
        }

        return spatialMap;
    }

    public SimpleFeatureCollection getGranuleSet(String granularityName) {
        //query from store
        ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
        return DataUtilities.collection(features);
    }
}
