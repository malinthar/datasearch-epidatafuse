package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

/**
 * For data fusion.
 */
public class FuseEngine {
    //aggregating
    private Aggregator aggregator;
    //granularityConvertor
    private GranularityConvertor granularityConvertor;
    //Transformer
    private Transformer transformer;

    public GranularityConvertor getGranularityConvertor() {
        if (this.granularityConvertor == null) {
            this.granularityConvertor = new GranularityConvertor();
        }
        return this.granularityConvertor;
    }
}

