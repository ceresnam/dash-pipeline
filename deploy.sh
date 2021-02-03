#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ${0} service_account_key.json"
    exit 1
fi

# osx: export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
export PATH=$PATH:~/apache-maven-3.6.3/bin

export GOOGLE_APPLICATION_CREDENTIALS=${1}

mvn compile exec:java \
    -Dexec.mainClass=com.modrykonik.dash.DashPipeline \
    -Dexec.args="\
        --runner=DataflowRunner \
        --project=maximal-beach-125109 \
        --stagingLocation=gs://dash_pipelines/staging \
        --templateLocation=gs://dash_pipelines/templates/DashPipeline \
    "

gcloud auth activate-service-account --key-file ${1}
gsutil cp src/main/java/com/modrykonik/dash/DashPipeline_metadata gs://dash_pipelines/templates/

