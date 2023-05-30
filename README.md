# Custom Transformation
Custom Transformations (SMTs) applied in source and sink connectors. Before transformation, message need to be in a desired format with respect to predefined transformers.

## Steps:

### Install Maven
Install the maven to include dependencies in the pom.xml. Then simply run,
```shell
mvn clean install
```
Custom-smt module is there. After building the project the dependency jars and module jar will be created in the
custom-smt-module/custom-smt directory.

After that you can commit your changes.

```shell
You should create the release tag and use it in the docker compose file for every new release.
```

### Docker Build

Check the docker-compose.yml file before proceed the build. There are variables that need 
to be correct before deployment.

```shell
curl --retry 3 -o  repo https://codeload.github.com/PeerIslands/kafkatransforms/tar.gz/refs/tags/tag_v1
        tar -xf repo
        cd kafkatransforms-tag_v1/custom-smt-module
        cp -r custom-smt/   /usr/share/confluent-hub-components
```
## Note
If you want the latest changes , you have to create the tag in github and use that tag version in the above curl url, and the repo name on the command eg: kafkatransforms-${tag_version} 



