<p align="center">
    <a href="https://www.witboost.com/">
        <img src="docs/img/witboost_logo.svg" alt="witboost" width=600 >
    </a>
</p>

Designed by [Agile Lab](https://www.agilelab.it/), witboost is a versatile platform that addresses a wide range of sophisticated data engineering challenges. It enables businesses to discover, enhance, and productionize their data, fostering the creation of automated data platforms that adhere to the highest standards of data governance. Want to know more about witboost? Check it out [here](https://www.witboost.com/) or [contact us!](https://witboost.com/contact-us)

This repository is part of our [Starter Kit](https://github.com/agile-lab-dev/witboost-starter-kit) meant to showcase witboost's integration capabilities and provide a "batteries-included" product.

## Airbyte Specific Provisioner

- [Overview](#overview)
- [Building](#building)
- [Running](#running)
- [Configuring](#configuring)
- [Deploying](#deploying)
- [HLD](docs/HLD.md)
- [API specification](docs/API.md)

## Overview

This project implements a simple Specific Provisioner for Airbyte Platform. After deploying this microservice and configuring witboost to use it, the platform can deploy components that use Airbyte to perform Extract and Load of Data into a Warehouse. Supported Components are: Airbyte Workload.

Refer to the [witboost Starter Kit repository](https://github.com/agile-lab-dev/witboost-starter-kit) for information on the templates that can be used with this Specific Provisioner.

### What's a Specific Provisioner?

A Specific Provisioner is a microservice which is in charge of deploying components that use a specific technology. When the deployment of a Data Product is triggered, the platform generates it descriptor and orchestrates the deployment of every component contained in the Data Product. For every such component the platform knows which Specific Provisioner is responsible for its deployment, and can thus send a provisioning request with the descriptor to it so that the Specific Provisioner can perform whatever operation is required to fulfill this request and report back the outcome to the platform.

You can learn more about how the Specific Provisioners fit in the broader picture [here](https://docs.witboost.agilelab.it/docs/p2_arch/p1_intro/#deploy-flow).

### Airbyte

Airbyte is an open-source data integration platform that syncs data from various sources to destinations like data warehouses, databases, and data lakes. It provides data engineering teams with a comprehensive set of tools based on a modular architecture to handle necessary tasks and operations from a single platform.

The main features of Airbyte are as follows:

- Data Stream Selection: Airbyte allows you to select the specific API streams you want to replicate, giving you control over the data you want to integrate.
- Connector Development Kit (CDK): Airbyte's CDK simplifies the process of building connectors, allowing you to create connectors in a short amount of time with minimal code.
- Data Extraction and Loading: Airbyte provides a secure and reliable way to extract data from different tools and load it into your desired data storage.
- Airbyte is self-hosted, giving you full control over your data and associated costs. It offers a cost-effective alternative to other ETL tools, as it is not volume-based in terms of pricing.

Learn more about it on the [official website](https://docs.airbyte.com/)

### Software Stack

This microservice is written in Scala 2.13, using Akka HTTP (pre-license change) for the HTTP layer. Project is built with SBT and supports packaging as JAR, fat-JAR and Docker image, ideal for Kubernetes deployments (which is the preferred option).


## Building

**Requirements:**

- Java 17 (11 works as well)
- SBT
- Node
- Docker (for building images only)


**Generating sources:** this project uses OpenAPI as standard API specification and the [OpenAPI Generator](https://openapi-generator.tech) CLI to generate server code from the specification.

In a terminal, install the OpenAPI Generator CLI and run the `generateCode` SBT task:

```bash
npm install @openapitools/openapi-generator-cli -g
sbt generateCode
```

*Note:* the `generateCode` SBT task needs to be run again if `clean` or similar tasks are executed.

**Compiling:** is handled by the standard task:

```bash
sbt compile
```

**Tests:** are handled by the standard task as well:

```bash
sbt test
```

**Artifacts & Docker image:** the project uses SBT Native Packager for packaging. Build artifacts with:

```
sbt package
```

The Docker image can be built with:

```
sbt docker:publishLocal
```

*Note:* the version for the project is automatically computed using information gathered from Git, using branch name and tags. Unless you are on a release branch `1.2.x` or a tag `v1.2.3` it will end up being `0.0.0`. You can follow this branch/tag convention or update the version computation to match your preferred strategy.

**CI/CD:** the pipeline is based on GitLab CI as that's what we use internally. It's configured by the `.gitlab-ci.yaml` file in the root of the repository. You can use that as a starting point for your customizations.

## Running

To run the server locally, use:

```bash
sbt generateCode compile run
```

By default, the server binds to port 8093 on localhost. After it's up and running you can make provisioning requests to this address. You can also check the API documentation served [here](http://127.0.0.1:8093/datamesh.airbytespecificprovisioner/0.0/swagger/docs/index.html).

### Run Airbyte locally

During development, it can be useful to have a local instance of Airbyte available for testing. To use Airbyte on your local machine, refer to the instructions provided [here](https://docs.airbyte.com/).

## Configuring

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` and some `application.conf` examples in the Helm chart (see below). Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs.

Airbyte information can be passed in via environment variables or configuration file; the mapping done by default resides in the `reference.conf` and is the following:

| Setting          | Defaults to Environment Variable |
|:-----------------|:---------------------------------|
| base-url         | AIRBYTE_BASE_URL                 |
| workspace-id     | AIRBYTE_WORKSPACE_ID             |
| source-id        | AIRBYTE_SOURCE_ID                |
| destination-id   | AIRBYTE_DESTINATION_ID           |
| git-token        | DBT_GIT_TOKEN                    |
| dbt-user         | DBT_GIT_USER                     |
| airbyte-user     | AIRBYTE_USERNAME                 |
| airbyte-password | AIRBYTE_PASSWORD                 |
| basic-auth       | AIRBYTE_BASIC_AUTH_ENABLED       |


### Async configuration

Enable async handling on the Airbyte Specific Provisioner

| Configuration                                 | Description                                                                                                                | Default |
|:----------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------|:--------|
| `dataMeshProvisioner.async.provision.enabled` | Enables the async provision/unprovision tasks. When enabled, operations will return 202 with a token to be used on polling | `false` |
| `dataMeshProvisioner.async.type`              | Defines the type of repository to be used to store the status of the asynchronous tasks. Allowed values: [`cache`]         | `cache` |

Logging is handled with Logback, you can find an example `logback.xml` in the Helm chart. Customize it and pass it using the `logback.configurationFile` system property.

## Deploying

This microservice is meant to be deployed to a Kubernetes cluster with the included Helm chart and the scripts that can be found in the `helm` subdirectory. You can find more details [here](helm/README.md).

## License

This project is available under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0); see [LICENSE](LICENSE) for full details.

## About Witboost

[Witboost](https://witboost.com/) is a cutting-edge Data Experience platform, that streamlines complex data projects across various platforms, enabling seamless data production and consumption. This unified approach empowers you to fully utilize your data without platform-specific hurdles, fostering smoother collaboration across teams.

It seamlessly blends business-relevant information, data governance processes, and IT delivery, ensuring technically sound data projects aligned with strategic objectives. Witboost facilitates data-driven decision-making while maintaining data security, ethics, and regulatory compliance.

Moreover, Witboost maximizes data potential through automation, freeing resources for strategic initiatives. Apply your data for growth, innovation and competitive advantage.

[Contact us](https://witboost.com/contact-us) or follow us on:

- [LinkedIn](https://www.linkedin.com/showcase/witboost/)
- [YouTube](https://www.youtube.com/@witboost-platform)
