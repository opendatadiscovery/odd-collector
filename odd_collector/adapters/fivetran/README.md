# Fivetran Adapter

## Configuration

- **name**: The name of the adapter (e.g. `fivetran`).
- **api_key**: The Fivetran API key.
- **api_secret**: The Fivetran API secret.
- **connector_id**: The ID of the Fivetran connector to use.
- **destination_id**: The ID of the Fivetran destination to use.

`connector_id` and `destination_id` are necessary to retrieve the configuration details (e.g. host, port, user, password) of the corresponding connectors and destinations. These details are used to obtain the ODDRNs of the relevant data sources. Note that the appropriate DataEntityLists must be present on the ODD platform, which can be accomplished by creating the necessary adapters and running them.


#### Please note 
Many Fivetran features are under development, so things could change in the future.