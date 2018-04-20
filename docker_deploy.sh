#!/bin/bash
docker login -u $DOCKER_USER_NAME -p $DOCKER_PASS_WORD
export REPO=uscisii2/etk
docker build -f Dockerfile -t $REPO:$TRAVIS_TAG .
docker tag $REPO:$TRAVIS_TAG $REPO:latest
docker push $REPO