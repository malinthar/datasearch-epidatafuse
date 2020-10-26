package io.datasearch.epidatafuse.core.fusionpipeline.datastore.ingestion.util;

import org.opengis.feature.simple.SimpleFeature;

import java.util.List;

/**
 * Interface for Transforms.
 */
public interface Transformer {
    public List<SimpleFeature> transform();
}
