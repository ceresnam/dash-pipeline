#!/bin/bash

# osx: export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
export PATH=$PATH:~/apache-maven-3.6.3/bin

mvn compile exec:java \
    -Dexec.mainClass=com.modrykonik.dash.DashPipeline \
    -Dexec.args="\
        --runner=DataflowRunner \
        --project=maximal-beach-125109 \
        --stagingLocation=gs://dash_pipelines/staging \
        --templateLocation=gs://dash_pipelines/templates/DashPipeline \
    "

gsutil cp src/main/java/com/modrykonik/dash/DashPipeline_metadata gs://dash_pipelines/templates/

