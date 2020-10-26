package io.datasearch.epidatafuse.core;

import io.datasearch.epidatafuse.core.fusionpipeline.FusionPipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * ServerContext , contextual environment for pipeline management.
 */
public class ServerContext {
    private static Map<String, FusionPipeline> pipelines = new HashMap<>();

    public static void addPipeline(String pipelineId, FusionPipeline pipeLine) {
        pipelines.put(pipelineId, pipeLine);
    }

    public static FusionPipeline getPipeline(String pipelineId) {
        return pipelines.get(pipelineId);
    }
}
