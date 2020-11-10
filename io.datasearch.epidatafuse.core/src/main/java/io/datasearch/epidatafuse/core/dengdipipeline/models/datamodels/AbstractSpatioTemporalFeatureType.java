package io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Data model for aggregated features
 */

public class AbstractSpatioTemporalFeatureType {

    private SimpleFeatureType featureType;
    private SimpleFeatureCollection featureCollection;
    private String temporalGranularity;
    private String spatialGranularity;

    public AbstractSpatioTemporalFeatureType(SimpleFeatureType featureType, SimpleFeatureCollection featureCollection,
                                             String temporalGranularity, String spatialGranularity) {
        this.featureType = featureType;
        this.featureCollection = featureCollection;
        this.temporalGranularity = temporalGranularity;
        this.spatialGranularity = spatialGranularity;
    }
}
