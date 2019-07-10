#!/bin/bash
if [[ $# != 1 ]]
then
  echo "Import LDBC SNB dataset into ArangoDB instance."
  echo ""
  echo "Usage: import.sh DATASET_DIR"
  echo "  DATASET_DIR     LDBC SNB dataset directory containing "
  echo "                  social_network/ directory. "
  echo "                  social_network_supplementary_files/ directories."

  exit
fi

dataset=$1

arangoimport --file ${dataset}/social_network/person_0_0.csv --type csv  --separator "|" --collection persons --create-collection true --translate "id=_key"
arangoimport --file ${dataset}/social_network/person_knows_person_0_0.csv --from-collection-prefix persons --to-collection-prefix persons --type csv --separator "|" --collection knows --translate "Person1.id=_from" --translate "Person2.id=_to"
