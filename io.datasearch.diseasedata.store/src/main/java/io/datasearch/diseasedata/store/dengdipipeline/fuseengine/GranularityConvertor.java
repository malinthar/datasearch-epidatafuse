package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For granularity conversion.
 */
public class GranularityConvertor {
    private static final Logger logger = LoggerFactory.getLogger(GranularityConvertor.class);

    private DataStore dataStore;

    public GranularityConvertor(DataStore dataStore) {
        this.dataStore = dataStore;
    }

//
//
//    public void temporalConversion(String featureType, String temporalMappingMethod) {
//
//    }
//
//    public void spatialConversion(String featureType, String spatialMappingMethod) {
//        if (spatialMappingMethod.equals("NearestPointGranularityMap")) {
//
//            String granulityType = "weatherstations";
//            NearestPointGranularityMap weatherStationMap = (NearestPointGranularityMap)
//                    this.spatialGranularityMap.get(granulityType);
//            try {
//                this.nearestPointGranularityMapConvertor(featureType,
//                        "ObservedValue", granulityType,
//                        "StationName", weatherStationMap
//                );
//            } catch (Exception e) {
//                logger.info(e.getMessage());
//            }
//        }
//    }
//
//
//    public void nearestPointGranularityMapConvertor(String featureType, String valueAttribute,
//                                                    String featureGranularityType,
//                                                    String featureGranularityTypeIndexCol,
//                                                    NearestPointGranularityMap spatialMap) throws Exception {
//
//        NearestPointsAggregator aggregator = new NearestPointsAggregator(this.dataStore);
//        aggregator.nearestPointGranularityMapConvertor(featureType, valueAttribute,
//                featureGranularityType, featureGranularityTypeIndexCol, spatialMap);
//    }
}
