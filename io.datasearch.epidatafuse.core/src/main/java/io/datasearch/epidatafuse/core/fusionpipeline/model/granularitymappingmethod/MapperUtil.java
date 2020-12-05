package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import java.util.HashMap;
import java.util.Map;

/**
 * MapperUtils.
 */
public class MapperUtil {
    private static final Map<String, Map<String, Object>> MAPPERS = new HashMap<>();

    static {
        MAPPERS.put(NearestMapper.MAPPER_NAME, NearestMapper.getArguments());
        MAPPERS.put(ContainMapper.MAPPER_NAME, ContainMapper.getArguments());
        MAPPERS.put(WithinRadiusMapper.MAPPER_NAME, ContainMapper.getArguments());
    }

    public static Map<String, Map<String, Object>> getMAPPERS() {
        return MAPPERS;
    }

    public static Map<String, Object> getMapper(String mapperName) {
        return MAPPERS.get(mapperName);
    }
}
