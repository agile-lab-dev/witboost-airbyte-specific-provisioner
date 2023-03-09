## Airbyte Specific Provisioner

This project is an Airbyte specific provisioner to be used in the sandbox environment.

This project uses OpenAPI as standard API specification and the [OpenAPI Generator](https://openapi-generator.tech)

### To compile and generate APIs

```bash
brew install sbt
brew install node
npm install @openapitools/openapi-generator-cli -g
sbt generateCode compile
export WITBOOST_MESH_TOKEN= ... # Ask the project administrator for the token
```

To set permanently the env variable `WITBOOST_MESH_TOKEN`, add the export command in ~/.bashrc

### Configure Intellij
- open the project in IntelliJ
  - accept the "load sbt project" notification in the bottom right corner
  - click on "reload all sbt project" button on the top right window
  - in the package it.agilelab.datamesh.airbytespecificprovisioner.server
    - run the server Main class (by IntelliJ) (see # Run the app)
    - verify that some logs are printed on the Main window in IntelliJ
    
- On Preferences/Code Style/Scala 
  - choose "Scalafmt" as Formatter
  - check the "Reformat on file save" option

### Run the app and launching tests

```bash
sbt             # to enter the sbt console
generateCode    # to be aligned with recent changes to the API
compile
run
```

### API UI from browser
- When the app is running use the following link to access the API [swagger](http://127.0.0.1:8093/datamesh.airbytespecificprovisioner/0.0/swagger/docs/index.html)

### Testing the Feature Development
- Whenever you're trying to develop a feature (or) fixing some bugs, and you want to test the same in the local environment, you need to do the following:
  - Start the Airbyte UI locally (If you don't have the Airbyte hosted) by following this link - [How to Get Started with Local Development](https://docs.airbyte.com/contributing-to-airbyte/developing-locally/)
  - In the [reference.conf](src/main/resources/reference.conf) file, provide the information about **Snowflake** and **Airbyte**.
  - Example:
  ```
  snowflake {
  host = ${?SNOWFLAKE_HOST}
  host = "https://tc87458-popeye.snowflakecomputing.com"
  user = ${?SNOWFLAKE_USER}
  user = "ADMIN"
  password = ${?SNOWFLAKE_PASSWORD}
  password = "ChangeMe123"
  role = ${?SNOWFLAKE_ROLE}
  role = "ACCOUNTADMIN"
  warehouse = ${?SNOWFLAKE_WAREHOUSE}
  warehouse = "POPEYE"
  }
  ```
  - There are two ways in which you can perform your tests:
    - If the Airbyte UI is fitted with Basic Authentication, you need to set the value of `basic-auth` to `'true'` (Note that the value should be enclosed inside **single quotes**).
      - Example - ```basic-auth = 'true'```
    - If the Airbyte UI doesn't have any Authentication, you need to set the value of `basic-auth` to `'false'`
      - Example - ```basic-auth = 'false'```
  - To get information about how to fill in the airbyte values (For example - source-id, destination-id etc.), [refer to this documentation](https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html) where you get the list of APIs which you can send through Postman.
  - In order to get the information about `dbt-user` and `git-token`, contact the administrator.