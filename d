[1mdiff --git a/src/main/java/io/datasearch/diseasedata/store/RequestHandler.java b/src/main/java/io/datasearch/diseasedata/store/RequestHandler.java[m
[1mindex c21d1bd..e85e76c 100644[m
[1m--- a/src/main/java/io/datasearch/diseasedata/store/RequestHandler.java[m
[1m+++ b/src/main/java/io/datasearch/diseasedata/store/RequestHandler.java[m
[36m@@ -7,7 +7,7 @@[m [mimport org.slf4j.Logger;[m
 import org.slf4j.LoggerFactory;[m
 import org.springframework.web.bind.annotation.RequestMapping;[m
 import org.springframework.web.bind.annotation.RestController;[m
[31m-[m
[32m+[m[32mimport io.datasearch.denguestore.ingestion.DataIngester;[m
 import java.util.HashMap;[m
 import java.util.Map;[m
 [m
[36m@@ -47,6 +47,13 @@[m [mpublic class RequestHandler {[m
 [m
     @RequestMapping("/ingest")[m
     public String ingestDengDIpipeline() {[m
[31m-        return "Not implemented yet";[m
[32m+[m[32m        try{[m
[32m+[m[32m            String pipelineName = "dengue";[m
[32m+[m[32m            dengDIPipeLineMap.get(pipelineName).ingest();[m
[32m+[m[32m            return "Success";[m
[32m+[m[32m        }catch (Exception e) {[m
[32m+[m[32m            logger.error(e.getMessage());[m
[32m+[m[32m            return e.getMessage();[m
[32m+[m[32m        }[m
     }[m
 }[m
[1mdiff --git a/src/main/java/io/datasearch/diseasedata/store/dengdipipeline/DengDIPipeLine.java b/src/main/java/io/datasearch/diseasedata/store/dengdipipeline/DengDIPipeLine.java[m
[1mindex 88f1874..50f3953 100644[m
[1m--- a/src/main/java/io/datasearch/diseasedata/store/dengdipipeline/DengDIPipeLine.java[m
[1m+++ b/src/main/java/io/datasearch/diseasedata/store/dengdipipeline/DengDIPipeLine.java[m
[36m@@ -7,7 +7,7 @@[m [mimport io.datasearch.diseasedata.store.schema.SimpleFeatureTypeSchema;[m
 import org.geotools.data.DataStore;[m
 import org.slf4j.Logger;[m
 import org.slf4j.LoggerFactory;[m
[31m-[m
[32m+[m[32mimport io.datasearch.denguestore.ingestion.DataIngester;[m
 import java.util.Map;[m
 [m
 /**[m
[36m@@ -46,4 +46,13 @@[m [mpublic class DengDIPipeLine {[m
         }[m
         return this.fuseEngine;[m
     }[m
[32m+[m
[32m+[m[32m    public void ingest(){[m
[32m+[m[32m        try{[m
[32m+[m[32m            DataIngester dataIngester = new DataIngester();[m
[32m+[m[32m            dataIngester.insertData(this.getDataStore(),this.simpleFeatureTypeSchemas);[m
[32m+[m[32m        }catch(Exception e){[m
[32m+[m[32m            logger.error(e.getMessage());[m
[32m+[m[32m        }[m
[32m+[m[32m    }[m
 }[m
[1mdiff --git a/src/main/java/io/datasearch/diseasedata/store/schema/SimpleFeatureTypeSchema.java b/src/main/java/io/datasearch/diseasedata/store/schema/SimpleFeatureTypeSchema.java[m
[1mindex 452cebf..c203cf5 100644[m
[1m--- a/src/main/java/io/datasearch/diseasedata/store/schema/SimpleFeatureTypeSchema.java[m
[1m+++ b/src/main/java/io/datasearch/diseasedata/store/schema/SimpleFeatureTypeSchema.java[m
[36m@@ -13,11 +13,13 @@[m [mpublic class SimpleFeatureTypeSchema implements DiseaseDataSchema {[m
     private SimpleFeatureType sft = null;[m
     private String typeName;[m
     private List<Map<String, String>> attributes;[m
[32m+[m[32m    private Map<String, String> configurations;[m
 [m
 [m
     public SimpleFeatureTypeSchema(Map<String, Object> parameters) {[m
         this.typeName = (String) parameters.get("feature_name");[m
         this.attributes = (List<Map<String, String>>) parameters.get("attributes");[m
[32m+[m[32m        this.configurations = (Map<String, String>) parameters.get("configurations");[m
         buildSimpleFeature(this.attributes);[m
     }[m
 [m
[36m@@ -31,6 +33,15 @@[m [mpublic class SimpleFeatureTypeSchema implements DiseaseDataSchema {[m
         return sft;[m
     }[m
 [m
[32m+[m[32m    public List<Map<String, String>> getAttributes(){[m
[32m+[m[32m        return this.attributes;[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m     public Map<String, String> getConfigurations(){[m
[32m+[m[32m        return this.configurations;[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m
     public void buildSimpleFeature(List<Map<String, String>> attributes) {[m
         if (sft == null) {[m
             StringBuilder featureAttributes = new StringBuilder();[m
