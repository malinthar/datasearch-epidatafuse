package io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Data model for temporal aggregated feature collection
 */

public class TemporallyAggregatedCollection {
    private SimpleFeatureType featureType;
    private SimpleFeatureCollection featureCollection;
    private String temporalGranularity;
    private String dtg;

    public TemporallyAggregatedCollection(SimpleFeatureType featureType, SimpleFeatureCollection featureCollection,
                                          String temporalGranularity,
                                          String dtg) {
        this.featureType = featureType;
        this.featureCollection = featureCollection;
        this.temporalGranularity = temporalGranularity;
        this.dtg = dtg;
    }

    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    public SimpleFeatureCollection getFeatureCollection() {
        return featureCollection;
    }

    public String getTemporalGranularity() {
        return temporalGranularity;
    }

    public String getDtg() {
        return dtg;
    }
}
