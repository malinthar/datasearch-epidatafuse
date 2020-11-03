package io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels;

import org.geotools.data.simple.SimpleFeatureCollection;

/**
 * Data model for temporal aggregated feature collection
 */

public class SpatiallyAggregatedCollection {
    private String featureType;
    private SimpleFeatureCollection featureCollection;
    private String spatialGranularity;
    private String temporalGranularity;
    private String dtg;

    public SpatiallyAggregatedCollection(String featureType, SimpleFeatureCollection featureCollection,
                                         String spatialGranularity,
                                         String temporalGranularity, String dtg) {
        this.featureType = featureType;
        this.featureCollection = featureCollection;
        this.spatialGranularity = spatialGranularity;
        this.temporalGranularity = temporalGranularity;
        this.dtg = dtg;
    }

    public String getFeatureType() {
        return featureType;
    }

    public SimpleFeatureCollection getFeatureCollection() {
        return featureCollection;
    }

    public String getSpatialGranularity() {
        return spatialGranularity;
    }

    public String getTemporalGranularity() {
        return temporalGranularity;
    }

    public String getDtg() {
        return dtg;
    }
}
