#!/bin/bash
docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
export REPO=uscisii2/etk
docker build -t $REPO:$TRAVIS_TAG .
docker tag $REPO:$TRAVIS_TAG $REPO:latest
docker push $REPO