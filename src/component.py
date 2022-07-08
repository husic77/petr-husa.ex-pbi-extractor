import logging
import requests
import pandas
import time

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_CLIENT_ID = '#client_id'
KEY_PASSWORD = '#password'
KEY_USERNAME = '#username'
KEY_INCREMENTAL = 'incremental'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_PASSWORD, KEY_USERNAME, KEY_INCREMENTAL]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.access_token = None
        self.get_api_token()
        self.incremental = self.get_incremental()

    def get_incremental(self):
        params = self.configuration.parameters
        return params.get(KEY_INCREMENTAL)

    def get_api_token(self):
        params = self.configuration.parameters

        url = "https://login.microsoftonline.com/common/oauth2/token"
        body = {
            "Content-Type": "application/x-www-form-urlencoded",
            "client_id": params.get(KEY_CLIENT_ID),
            "scope": "openid",
            "resource": "https://analysis.windows.net/powerbi/api",
            "grant_type": "password",
            "password": params.get(KEY_PASSWORD),
            "username": params.get(KEY_USERNAME)
        }

        response = requests.post(url, data=body).json()
        self.access_token = response['access_token']

    def get_pbi_groups(self):
        url = "https://api.powerbi.com/v1.0/myorg/groups"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = requests.get(url, headers=headers).json()

        # Create output table (Table-definition - just metadata)
        table = self.create_out_table_definition('pbi_groups.csv', incremental=self.incremental,
                                                 primary_key=['name', 'id'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        columns_to_write = ["id", "name"]
        to_write = pandas.DataFrame.from_dict(response["value"])
        to_write.to_csv(table.full_path, index=False, columns=columns_to_write)

        self.write_manifest(table)

    def get_pbi_users(self):
        # Create output table (Table-definition - just metadata)

        keys = ["email", "group_user_access_right", "display_name", "identifier", "principal_type", "groups_id_parent"]
        table = self.create_out_table_definition('pbi_users.csv', incremental=self.incremental, columns=keys,
                                                 primary_key=['email', 'group_user_access_right', 'groups_id_parent'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_groups.csv") as f:
            file_data = pandas.read_csv(f)
            pd = pandas.DataFrame(file_data)
            group_id_total = pd["id"].to_list()

        for groupId in group_id_total:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/users"
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }

            response = requests.get(url, headers=headers).json()
            pd = pandas.DataFrame.from_dict(response["value"])
            new_items = {
                "email": pd.get('emailAddress'),
                "group_user_access_right": pd.get('groupUserAccessRight'),
                "display_name": pd.get('displayName'),
                "identifier": pd.get('identifier'),
                "principal_type": pd.get('principalType'),
                "groups_id_parent": groupId,
            }

            to_write = pandas.DataFrame.from_dict(new_items)
            # print(to_write)
            to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

            self.write_manifest(table)

    def get_pbi_datasets(self):
        # Create output table (Table-definition - just metadata)
        keys = ["name",
                "id",
                "configured_by",
                "is_refreshable",
                "is_effective_identity_required",
                "is_effective_identity_roles_required",
                "is_on_prem_gateway_required",
                "target_storage_mode",
                "create_report_embed_url",
                "qna_embed_url",
                "group_id_parent"]
        table = self.create_out_table_definition('pbi_datasets.csv', incremental=self.incremental, columns=keys,
                                                 primary_key=['name', 'id'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_groups.csv") as f:
            file_data = pandas.read_csv(f)
            pd = pandas.DataFrame(file_data)
            group_id_total = pd["id"].to_list()

        for groupId in group_id_total:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets"
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }
            response = requests.get(url, headers=headers).json()
            pd = pandas.DataFrame.from_dict(response["value"])

            if not pd.empty:
                try:
                    new_items = {
                        "name": pd.get('name'),
                        "id": pd.get('id'),
                        "configured_by": pd.get('configuredBy'),
                        "is_refreshable": pd.get('isRefreshable'),
                        "is_effective_identity_required": pd.get('isEffectiveIdentityRequired'),
                        "is_effective_identity_roles_required": pd.get('isEffectiveIdentityRolesRequired'),
                        "is_on_prem_gateway_required": pd.get('isOnPremGatewayRequired'),
                        "target_storage_mode": pd.get('targetStorageMode'),
                        "create_report_embed_url": pd.get('createReportEmbedURL'),
                        "qna_embed_url": pd.get('qnaEmbedURL'),
                        "group_id_parent": groupId
                    }
                except AttributeError:
                    pass
                else:
                    to_write = pandas.DataFrame.from_dict(new_items)
                    # print(to_write)
                    to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

    def get_pbi_dashboards(self):
        # Create output table (Table-definition - just metadata)
        keys = [
            "id",
            "display_name",
            "is_read_only",
            "web_url",
            "embed_url",
            "group_id_parent"
        ]
        table = self.create_out_table_definition('pbi_dashboards.csv', incremental=self.incremental, columns=keys,
                                                 primary_key=['id'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_groups.csv") as f:
            file_data = pandas.read_csv(f)
            pd = pandas.DataFrame(file_data)
            group_id_total = pd["id"].to_list()

        for groupId in group_id_total:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/dashboards"
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }

            response = requests.get(url, headers=headers).json()

            pd = pandas.DataFrame.from_dict(response["value"])

            if not pd.empty:
                try:
                    new_items = {
                        "id": pd.get('id'),
                        "display_name": pd.get('displayName'),
                        "is_read_only": pd.get('isReadOnly'),
                        "web_url": pd.get('webUrl'),
                        "embed_url": pd.get('embedUrl'),
                        "group_id_parent": groupId
                    }
                except AttributeError:
                    pass
                else:
                    to_write = pandas.DataFrame.from_dict(new_items)
                    # print(to_write)
                    to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

    def get_pbi_reports(self):
        # Create output table (Table-definition - just metadata)
        keys = [
            "id",
            "report_type",
            "name",
            "web_url",
            "embed_url",
            "is_from_pbix",
            "is_owned_by_me",
            "dataset_id",
            "group_id_parent"
        ]
        table = self.create_out_table_definition('pbi_reports.csv', incremental=self.incremental, columns=keys,
                                                 primary_key=['id'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_groups.csv") as f:
            file_data = pandas.read_csv(f)
            pd = pandas.DataFrame(file_data)
            group_id_total = pd["id"].to_list()

        for groupId in group_id_total:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/reports"
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }

            response = requests.get(url, headers=headers).json()

            pd = pandas.DataFrame.from_dict(response["value"])

            if not pd.empty:
                try:
                    new_items = {
                        "id": pd.get('id'),
                        "report_type": pd.get('reportType'),
                        "name": pd.get('name'),
                        "web_url": pd.get('webUrl'),
                        "embed_url": pd.get('embedUrl'),
                        "is_from_pbix": pd.get('isFromPbix'),
                        "is_owned_by_me": pd.get('isOwnedByMe'),
                        "dataset_id": pd.get('datasetId'),
                        "group_id_parent": groupId
                    }
                except AttributeError:
                    pass
                else:
                    to_write = pandas.DataFrame.from_dict(new_items)
                    # print(to_write)
                    to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

    def get_pbi_gateways(self):
        keys = [
            "id",
            "gateway_id",
            "name",
            "type",
            "public_key_exponent",
            "public_key_modulus",
            "gateway_annotation"
        ]
        table = self.create_out_table_definition('pbi_gateways.csv', incremental=self.incremental, columns=keys,
                                                 primary_key=['id'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        url = "https://api.powerbi.com/v1.0/myorg/gateways"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = requests.get(url, headers=headers).json()
        pd = pandas.DataFrame.from_dict(response["value"])
        pk_details = pd.get("publicKey")
        gw = pd.get('gatewayAnnotation').to_dict()

        if not pd.empty:
            try:
                new_items = {
                    "id": pd.get('id'),
                    "gateway_id": pd.get('gatewayId'),
                    "name": pd.get('name'),
                    "type": pd.get('type)'),
                    "public_key_exponent": pk_details.get('exponent'),
                    "public_key_modulus": pk_details.get('modulus'),
                    "gateway_annotation": str(gw[0])
                }
            except AttributeError:
                print("error")
            else:
                to_write = pandas.DataFrame.from_dict(new_items)
                # print(to_write)
                to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

    def get_pbi_datasources_gateway(self):
        # Create output table (Table-definition - just metadata)
        keys = [
            "id",
            "gateway_id",
            "datasource_type",
            "connection_details",
            "credential_type",
            "credential_details_use_end_user_oauth2_credentials",
            "datasource_name"
        ]
        table = self.create_out_table_definition('pbi_datasources_gateway.csv', incremental=self.incremental,
                                                 columns=keys,
                                                 primary_key=['id'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_gateways.csv") as f:
            file_data = pandas.read_csv(f)
            pd = pandas.DataFrame(file_data)
            group_id_total = pd["id"].to_list()

        for gatewayId in group_id_total:
            url = f"https://api.powerbi.com/v1.0/myorg/gateways/{gatewayId}/datasources"
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }

            response = requests.get(url, headers=headers).json()
            # print(response)
            pd = pandas.DataFrame.from_dict(response["value"])
            credential_details = pd.get('credentialDetails')
            if not pd.empty:
                try:
                    new_items = {
                        "id": pd.get('id'),
                        "gateway_id": gatewayId,
                        "datasource_type": pd.get('datasourceType'),
                        "connection_details": pd.get('connectionDetails'),
                        "credential_type": pd.get('credentialType'),
                        "credential_details_use_end_user_oauth2_credentials":
                            credential_details.get('useEndUserOAuth2Credentials'),
                        "datasource_name": pd.get('datasourceName')
                    }
                except AttributeError:
                    pass
                else:
                    to_write = pandas.DataFrame.from_dict(new_items)
                    # print(to_write)
                    to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

    def get_pbi_datasets_refreshes(self):
        # Create output table (Table-definition - just metadata)
        keys = [
            "id",
            "start_time",
            "end_time",
            "status",
            "service_exception_json",
            "dataset_id_parent",
            "request_id",
            "refresh_type"
        ]
        table = self.create_out_table_definition('pbi_datasets_refreshes.csv', incremental=self.incremental,
                                                 columns=keys,
                                                 primary_key=['id', 'start_time', 'end_time', 'dataset_id_parent',
                                                              'request_id', 'refresh_type'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_datasets.csv") as f:
            file_data = pandas.read_csv(f, usecols=['id', 'group_id_parent', 'is_refreshable'])
            pd = pandas.DataFrame(file_data)

            group_dataset_all = pd.to_dict(orient='records')

        for key in range(len(group_dataset_all)):
            time.sleep(1)
            group_id = group_dataset_all[key]['group_id_parent']
            dataset_id = group_dataset_all[key]['id']

            if group_dataset_all[key]['is_refreshable']:

                url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"
                headers = {
                    "Authorization": f"Bearer {self.access_token}"
                }

                response = requests.get(url, headers=headers).json()

                # print("pbi_datasets_refreshes:")
                # print(f"datasetID: {dataset_id}")
                # print(f"groupID: {group_id}")
                # print(response)

                try:
                    pd = pandas.DataFrame.from_dict(response["value"])
                    if not pd.empty:

                        try:
                            new_items = {
                                "id": pd.get('id'),
                                "start_time": pd.get('startTime'),
                                "end_time": pd.get('endTime'),
                                "status": pd.get('status'),
                                "service_exception_json": pd.get('serviceExceptionJson'),
                                "dataset_id_parent": dataset_id,
                                "request_id": pd.get('requestId'),
                                "refresh_type": pd.get('refreshType')
                            }

                        except AttributeError:
                            if len(pd) != 0:
                                print("pbi_datasets_refreshes - AttributeError:")
                                print(f"datasetID: {dataset_id}")
                                print(f"groupID: {group_id}")
                                print(response)
                            pass
                        else:
                            to_write = pandas.DataFrame.from_dict(new_items)
                            # print(to_write)
                            to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)
                except KeyError:
                    print("pbi_datasets_refreshes - KeyError:")
                    print(f"datasetID: {dataset_id}")
                    print(f"groupID: {group_id}")
                    pass

    def get_pbi_datasets_datasources(self):
        # Create output table (Table-definition - just metadata)

        keys = [
            "datasource_type",
            "connection_details_server",
            "connection_details_database",
            "connection_details_path",
            "connection_details_url",
            "connection_details_kind",
            "connection_details_connection_string",
            "datasource_id",
            "gateway_id",
            "name",
            "connection_string",
            "dataset_id_parent"
        ]
        table = self.create_out_table_definition('pbi_datasets_datasources.csv', incremental=self.incremental,
                                                 columns=keys,
                                                 primary_key=['datasource_id', 'gateway_id', 'name',
                                                              'dataset_id_parent'])

        self.write_manifest(table)

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        with open(out_table_path, "w"):
            pd = pandas.DataFrame(columns=keys)
            pd.to_csv(out_table_path, columns=keys, index=False)

        with open("../data/out/tables/pbi_datasets.csv") as f:
            file_data = pandas.read_csv(f, usecols=['id', 'group_id_parent', 'is_refreshable'])
            pd = pandas.DataFrame(file_data)
            group_dataset_all = pd.to_dict(orient='records')

        for key in range(len(group_dataset_all)):
            group_id = group_dataset_all[key]['group_id_parent']
            dataset_id = group_dataset_all[key]['id']

            if group_dataset_all[key]['is_refreshable']:
                url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/datasources"
                headers = {
                    "Authorization": f"Bearer {self.access_token}"
                }

                response = requests.get(url, headers=headers).json()

                try:
                    pd = pandas.DataFrame.from_dict(response.get("value"))
                except AttributeError:
                    pass
                else:

                    for _ in range(len(pd)):
                        connection_details = pd.get('connectionDetails')

                        new_items = {
                            "datasource_type": pd.get('datasourceType'),
                            "connection_details_server": connection_details[_].get('server'),
                            "connection_details_database": connection_details[_].get('database'),
                            "connection_details_path": connection_details[_].get('path'),
                            "connection_details_url": connection_details[_].get('url'),
                            "connection_details_kind": connection_details[_].get('kind'),
                            "connection_details_connection_string": connection_details[_].get('connectionString'),
                            "datasource_id": pd.get('datasourceId'),
                            "gateway_id": pd.get('gatewayId'),
                            "name": "",
                            "connection_string": "",
                            "dataset_id_parent": dataset_id
                        }
                        to_write = pandas.DataFrame(new_items, index=[0])
                        # print(to_write)
                        to_write.to_csv(table.full_path, mode="a", header=False, index=False, columns=keys)

    def run(self):
        """
        Main execution code
        """

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        logging.info(f"Incremental = {self.incremental}")

        self.get_pbi_groups()
        self.get_pbi_users()
        self.get_pbi_datasets()
        self.get_pbi_dashboards()
        self.get_pbi_reports()
        self.get_pbi_gateways()
        self.get_pbi_datasources_gateway()
        self.get_pbi_datasets_refreshes()
        self.get_pbi_datasets_datasources()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
