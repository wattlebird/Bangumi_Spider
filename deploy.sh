echo "$TRAVIS_BUILD_DIR"
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker tag scrapyd wattlebird/scrapyd:$TRAVIS_TAG
docker tag scrapyd wattlebird/scrapyd:latest
docker push wattlebird/scrapyd:$TRAVIS_TAG
docker push wattlebird/scrapyd:latest
