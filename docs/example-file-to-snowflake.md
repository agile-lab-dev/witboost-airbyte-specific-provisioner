```yaml
dataProduct:
  dataProductOwnerDisplayName: Name Surname
  environment: development
  domain: finance
  kind: dataproduct
  id: urn:dmb:dp:finance:cashflow:0
  description: This represents all the operating cashflows generating incoming or outgoing liquidity
  devGroup: datameshplatform
  ownerGroup: name.surname_agilelab.it
  dataProductOwner: user:name.surname_agilelab.it
  email: name.surname@agilelab.it
  version: 0.2.0
  fullyQualifiedName: Liquidity Cash Flows
  name: CashFlow
  informationSLA: null
  maturity: Strategic
  useCaseTemplateId: urn:dmb:utm:dataproduct-template:0.0.0
  infrastructureTemplateId: urn:dmb:itm:cdp-aws-dataproduct-provisioner:1
  billing: {}
  tags: []
  specific: {}
  components:
    - kind: workload
      id: urn:dmb:cmp:finance:cashflow:0:cashflows-calculation
      description: It calculates the cashflows starting from all the deals in input
      useCaseTemplateId: urn:dmb:utm:dbt-standard:0.0.0
      infrastructureTemplateId: urn:dmb:itm:dbt-provisioner:1
      fullyQualifiedName: CashFlows Calculation
      name: CashFlows Calculation
      technology: dbt
      version: 0.0.0
      dependsOn:
        - urn:dmb:cmp:finance:cashflow:0:snowflake-component
      readsFrom: []
      tags: []
      specific:
        workspaceId: 94d9d6ac-4ae9-44f0-af49-8dbb0adfba90
        sources:
          source1:
            name: Public Places Assaults CSV
            sourceDefinitionId: 778daa7c-feaf-4db6-96f3-70fd645acc77
            connectionConfiguration:
              url: https://stats.govt.nz/assets/Uploads/Tools/CSV-files-for-download/analysis-public-place-assaults-sexual-assaults-and-robberies-2015-csv.csv
              format: csv
              provider:
                storage: HTTPS
                user_agent: false
              dataset_name: public places assaults
        destinations:
          destination1:
            name: Local CSV folder
            destinationDefinitionId: 424892c4-daac-4491-b35d-c6688ba547ba
            connectionConfiguration:
              host: myhost
              role: admin
              schema: myschema
              database: mydatabase
              username: myusername
              warehouse: mywarehouse
              credentials:
                password: mypwd
              loading_method:
                method: Internal Staging
        connections:
          - name: Public Places Assaults CSV <> Snowflake
            #namespaceDefinition: source
            #namespaceFormat: "${SOURCE_NAMESPACE}"
            #prefix: ''
            sourceId: a4183d9e-aaaa-433d-b1d4-de734a4026c0
            destinationId: 5a6ae731-444d-4990-9e97-85446217c523
            #operationIds:
            #- 8ea76ef9-ae24-4c03-995c-3cc7388db19b
            syncCatalog:
              streams:
                - stream:
                    name: public places assaults
                    #      jsonSchema:
                    #        type: object
                    #        "$schema": http://json-schema.org/draft-07/schema#
                    #        properties:
                    #          Index:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          Urban_area_type:
                    #            type:
                    #            - string
                    #            - 'null'
                    #          Region_2013_code:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          Region_2013_label:
                    #            type:
                    #            - string
                    #            - 'null'
                    #          Area_unit_2013_code:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          Area_unit_2013_label:
                    #            type:
                    #            - string
                    #            - 'null'
                    #          Urban_area_2013_code:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          Urban_area_2013_label:
                    #            type:
                    #            - string
                    #            - 'null'
                    #          Population_mid_point_2015:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          " Rate_per_10000_population ":
                    #            type:
                    #            - string
                    #            - 'null'
                    #          " Rate_ratio_NZ_average_rate ":
                    #            type:
                    #            - string
                    #            - 'null'
                    #          Victimisations_calendar_year_2015:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          Territorial_authority_area_2013_code:
                    #            type:
                    #            - number
                    #            - 'null'
                    #          Territorial_authority_area_2013_label:
                    #            type:
                    #            - string
                    #            - 'null'
                    supportedSyncModes:
                      - full_refresh
                    defaultCursorField: []
                    sourceDefinedPrimaryKey: []
                  config:
                    syncMode: full_refresh
                    cursorField: []
                    destinationSyncMode: overwrite
                    primaryKey: []
                    aliasName: public_places_assaults
                    selected: true
            scheduleType: manual
            status: active
componentIdToProvision: urn:dmb:cmp:finance:cashflow:0:cashflows-calculation
```