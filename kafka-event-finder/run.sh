#!/bin/bash

# Set JAVA_HOME to Java 11
#JAVA_HOME=
APP_JAR=./app/build/libs/eventfinder.jar


###
RED='\033[0;31m'
NC='\033[0m' # No Color
###

# Check if application.yaml exists in config directory
if [[ -f "./config/application.yaml" ]]; then
  ./gradlew :app:bootJar
  java -jar ${APP_JAR}
else
  >&2 echo -e "${RED}ERROR${NC} ./config/application.yaml required"
  exit 1
fi


