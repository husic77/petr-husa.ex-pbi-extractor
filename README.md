PBI extractor
=============

Getting data from Power BI via API.


Functionality notes
===================

Prerequisites
=============

Inputs required to update the access token:

Client ID
<br>Power BI username
<br>Power BI password

Features
========



===================

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

Output
======

<br>data\out\tables\pbi_dashboards.csv
<br>data\out\tables\pbi_dashboards_refresh.csv
<br>data\out\tables\pbi_datasets.csv
<br>data\out\tables\pbi_datasets_refresh.csv
<br>data\out\tables\pbi_datasets_datasources.csv
<br>data\out\tables\pbi_datasets_refresh_schedule_days.csv
<br>data\out\tables\pbi_datasets_refresh_schedule_enable.csv
<br>data\out\tables\pbi_datasets_refresh_schedule_times.csv
<br>data\out\tables\pbi_datasets_refreshes.csv
<br>data\out\tables\pbi_datasources_gateway.csv
<br>data\out\tables\pbi_gateways.csv
<br>data\out\tables\pbi_groups.csv
<br>data\out\tables\pbi_groups_refresh.csv
<br>data\out\tables\pbi_reports.csv
<br>data\out\tables\pbi_reports_actual.csv
<br>data\out\tables\pbi_users.csv

Used APIs:
=========
https://login.microsoftonline.com/common/oauth2/token - refresh token to get data from Power BI
https://api.powerbi.com/v1.0/myorg/groups
https://api.powerbi.com/v1.0/myorg/groups/{groupId}/users
https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets
https://api.powerbi.com/v1.0/myorg/groups/{groupId}/dashboards
https://api.powerbi.com/v1.0/myorg/groups/{groupId}/reports
https://api.powerbi.com/v1.0/myorg/gateways
https://api.powerbi.com/v1.0/myorg/gateways/{gatewayId}/datasources
https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes
https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/datasources
https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshSchedule

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/husic77/petr-husa.ex-pbi-extractor petr-husa.ex-pbi-extractor
cd petr-husa.ex-pbi-extractor
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
