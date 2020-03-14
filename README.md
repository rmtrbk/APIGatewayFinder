# APIGatewayFinder
This is a simple lambda that gets triggered by an API gateway event demonstrating how a REST call can retrieve values through a lambda function.

## Design
* `ESTransactionManagerImpl` manages all `Elastic Search` transactions.

* `DatabaseTransactionManagerSeviceImpl` manages all `DynamoDB` transactions.

* `ClientBuilderManager` utility class build an `Elasticsearch` and `DynamoDB` clients to access `Elasticsearch` and `DynamoDB` APIs.

* `PropertyManager` reads required properties from the environment and makes them available across the application.

## Configuring AWS Infrastructure


## How to test




