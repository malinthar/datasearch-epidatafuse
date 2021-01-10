package io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * AttributeUtil is a util class for building CQL attribute syntax.
 */
public class AttributeUtil {
    private static final String STRING = "String";
    private static final String INTEGER = "Integer";
    private static final String DOUBLE = "Double";
    private static final String LONG = "Long";
    private static final String FLOAT = "Float";
    private static final String BOOLEAN = "Boolean";
    private static final String UUID = "UUID";
    private static final String DATE = "Date";
    private static final String TIMESTAMP = "Timestamp";
    private static final String POINT = "Point";
    private static final String LINESTRING = "LineString";
    private static final String POLYGON = "Polygon";
    private static final String MULTI_POINT = "MultiPoint";
    private static final String MULTI_LINE_STRING = "MultiLineString";
    private static final String MULTI_POLYGON = "MultiPolygon";
    private static final String GEOMETRY_COLLECTION = "GeometryCollection";
    private static final String GEOMETRY = "Geometry";
    private static final String LIST = "List[A]";
    private static final String MAP = "List[A,B]";
    private static final String BYTES = "Bytes";
    private static final String ATTRIBUTE_SEPARATOR = ":";
    public static final String SPATIAL_GRANULE_ATTRIBUTE = "spatialGranule";
    public static final String GEOMETRY_ATTRIBUTE = "geom";
    public static final String TEMPORAL_GRANULE_ATTRIBUTE = "temporalGranule";
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";
    private static final String ATTRIBUTE_TYPE_KEY = "attribute_type";
    private static final String INDEXED_KEY = "indexed";
    private static final String DEFAULT_INDEX = "default=true";
    private static final String SRID_ENTRY = "srid = 4326";
    private static final String INDEXED = "index=true";

    private static final List<String> ATTRIBUTE_TYPE_LIST =
            Arrays.asList(STRING, INTEGER, DOUBLE, LONG, FLOAT, BOOLEAN, UUID, DATE, TIMESTAMP,
                    POINT, LINESTRING, POLYGON, MULTI_POINT, MULTI_LINE_STRING,
                    MULTI_POLYGON, GEOMETRY_COLLECTION, GEOMETRY, LIST, MAP, BYTES);
    private static final List<String> GEOMETRIC_TYPE_LIST = Arrays.asList(GEOMETRY,
            GEOMETRY_COLLECTION, POINT, POLYGON, LINESTRING, MULTI_POINT, MULTI_LINE_STRING, MULTI_POLYGON);
    private static final Map<String, String> ATTRIBUTE_TYPE_MAP;

    static {
        ATTRIBUTE_TYPE_MAP = new HashMap<>();
        ATTRIBUTE_TYPE_LIST.forEach(attribute -> ATTRIBUTE_TYPE_MAP.put(attribute, attribute));
    }

    public static String getAttribute(String attributeName, Boolean indexed, String attributeType) {
        if (attributeName != null && ATTRIBUTE_TYPE_MAP.get(attributeType) != null) {
            StringBuilder builder = new StringBuilder("");
            builder.append(attributeName).append(ATTRIBUTE_SEPARATOR).append(ATTRIBUTE_TYPE_MAP.get(attributeType));
            if (DATE.equals(attributeType) || TIMESTAMP.equals(attributeType)) {
                builder.append(ATTRIBUTE_SEPARATOR).append(DEFAULT_INDEX);
            } else if (GEOMETRIC_TYPE_LIST.contains(attributeType)) {
                builder.append(ATTRIBUTE_SEPARATOR).
                        append(DEFAULT_INDEX).append(ATTRIBUTE_SEPARATOR).append(SRID_ENTRY);
            } else if (indexed) {
                builder.append(ATTRIBUTE_SEPARATOR).append(INDEXED);
            }
            return builder.toString();
        } else {
            return null;
        }
    }

    public static String getGranularityAttributes() {
        StringBuilder granularityAttributes = new StringBuilder();
        granularityAttributes.append(",").
                append(SPATIAL_GRANULE_ATTRIBUTE).
                append(ATTRIBUTE_SEPARATOR).
                append(STRING).append(ATTRIBUTE_SEPARATOR).append(INDEXED)
                .append(",").
                append(TEMPORAL_GRANULE_ATTRIBUTE).
                append(ATTRIBUTE_SEPARATOR).
                append(STRING).append(ATTRIBUTE_SEPARATOR).append(INDEXED);
        return granularityAttributes.toString();
    }

    public static Map<String, String> getSpatialGranuleAttribute() {
        Map<String, String> spatialGranuleAttribute = new HashMap<>();
        spatialGranuleAttribute.put(ATTRIBUTE_NAME_KEY, AttributeUtil.SPATIAL_GRANULE_ATTRIBUTE);
        spatialGranuleAttribute.put(ATTRIBUTE_TYPE_KEY, AttributeUtil.STRING);
        spatialGranuleAttribute.put(INDEXED_KEY, "true");
        return spatialGranuleAttribute;
    }

    public static String getGeometryAttributeWithType(String type) {
        StringBuilder geometryAttribute = new StringBuilder();
        geometryAttribute.append(",").
                append(GEOMETRY_ATTRIBUTE).
                append(ATTRIBUTE_SEPARATOR).
                append(ATTRIBUTE_TYPE_MAP.get(type));
        return geometryAttribute.toString();
    }

    public static Map<String, String> getGeometryAttribute() {
        Map<String, String> geometryAttribute = new HashMap<>();
        geometryAttribute.put(ATTRIBUTE_NAME_KEY, AttributeUtil.GEOMETRY_ATTRIBUTE);
        return geometryAttribute;
    }


    public static Map<String, String> getTemporalGranuleAttribute() {
        Map<String, String> temporalGranuleAttribute = new HashMap<>();
        temporalGranuleAttribute.put(ATTRIBUTE_NAME_KEY, AttributeUtil.TEMPORAL_GRANULE_ATTRIBUTE);
        temporalGranuleAttribute.put(ATTRIBUTE_TYPE_KEY, AttributeUtil.STRING);
        temporalGranuleAttribute.put(INDEXED_KEY, "true");
        return temporalGranuleAttribute;
    }

    public static Object convert(String value, String type) {
        if (ATTRIBUTE_TYPE_MAP.get(type) != null) {
            Object attributeValue;
            switch (type) {
                case FLOAT:
                    attributeValue = Float.parseFloat(value);
                    return attributeValue;
                case DATE:
                    DateTimeFormatter dateFormat =
                            DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);
                    attributeValue = Date.from(LocalDate.parse(value, dateFormat)
                            .atStartOfDay(ZoneOffset.UTC).toInstant());
                    break;
                case TIMESTAMP:
                    DateTimeFormatter timeStampFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmm", Locale.US);
                    attributeValue =
                            Date.from(LocalDate.parse(value, timeStampFormat).atStartOfDay(ZoneOffset.UTC).toInstant());
                    break;
                default:
                    attributeValue = value;
            }
            return attributeValue;
        } else {
            return value;
        }
    }

    public static List<String> getGeometricTypeList() {
        return GEOMETRIC_TYPE_LIST;
    }

    public static List<String> getAttributeTypeList() {
        return ATTRIBUTE_TYPE_LIST;
    }
}
