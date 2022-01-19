# README

hyper data adapter using AWS DynamoDB as service

## About

This adapter uses single table design. All data for this adapter is stored in a single DynamoDB table with a partition key named "pk" and a sort key named "sk".
The DynamoDB Table Name is defined as an environment variable, DynamoDbTable
The tablename passed from hyper to this adapter becomes the "pk"
The id passed from hyper to this adapter becomes the "sk"
Everything else is stored in normal document format.

**Note: as of Nov. 2021, use deno version 1.15.3 to avoid conflicts with https://x.nest.land/hyper-app-opine
`deno upgrade --version 1.15.3`

## How to setup and use with gitpod

1. Create a DynamoDB table on AWS. The partion key is string type and named "pk". The sort key is string type and named "sk".
2. Create an AWS IAM user with programmatic access and the ability to perform actions on this table.
3. On gitpod, configure 4 environment variables (https://www.gitpod.io/docs/environment-variables)
awsAccessKeyId
awsSecretKey
region 
DynamoDbTable
4. Spin up a gitpod instance by appending "gitpod.io/#" to the repo (gitpod.io/#https://github.com/cawilson1/hyper-adapter-dynamodb). You now have a coding environment with the DynamoDB environment variables injected.
5. Downgrade deno to a non-conflicting version: `deno upgrade --version 1.15.3`
6. Start the dev env by running "./scripts/harness.sh"
7. Do cool stuff



## Use with hyper-test
1. In a terminal, start hyper "./scripts/harness.sh"
2. In another terminal, run `HYPER=http://localhost:6363/test deno test --allow-net --allow-env --import-map=https://x.nest.land/hyper-test@0.0.2/import_map.json https://x.nest.land/hyper-test@0.0.2/mod.js`


## GH Actions
Add the following fields as secrets: 
AWSACCESSKEYID 
AWSSECRETKEY 
REGION 
TABLENAME 
Ensure that you have provisioned enough throughput for the DynamoDB table to pass the test.
