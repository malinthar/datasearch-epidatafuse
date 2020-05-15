#!/bin/sh
# launches datasearch-disease-data-store
java -cp target/dataserach-dengue-store-1.0-SNAPSHOT.jar io.datasearch.denguestore.DiseaseDataStore $1 $2 $3 $4 $5 $6
