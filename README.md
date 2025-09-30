# Custom Airflow Operator for Tableau Cloud API

This custom Airflow operator allows seamless triggering of Tableau Cloud **workbook** or **datasource** extract refreshes immediately after your data pipelines complete, ensuring your dashboards are always in sync with fresh data.

## Why Use This Operator?

Scheduling extract refresh for your workbook or data source on Tableau cloud is not the best approach when they rely on a data pipeline. This is because the scheduled extract refresh is not aware of your data pipeline run status and will go ahead to refresh the extract regardless whether your data pipeline runs successfully to refresh the underlying dataset or not. Also, the scheduled extract refresh only runs at the scheduled time even if the underlying dataset has been refreshed.

To automatically refresh workbook or data source after a successful Airflow data pipeline run, There is an official Apache Airflow Tableau provider. However, it does not support **Personal Access Token (PAT)** authentication, which is required for Tableau Cloud. But since there is a Tableau API for interacting with Tableau cloud resources, it means we can create our custom Airflow operator using this API.

This custom operator:
- Uses **PAT-based auth** securely via Airflow connections.
- Supports both **workbook** and **datasource** refreshes.
- Waits for refresh completion by default (optional).
- Integrates easily into your existing DAGs.

---

## Features
- Trigger extract refresh for workbooks or datasources
- Secure authentication using PAT stored in Airflow Connections
- Optional blocking refresh until completion
- Supports Tableau **Cloud**

---

## Requirements

- Python ≥ 3.8
- Airflow ≥ 2.7.0
- tableauserverclient (pip install for local development or add it to the list of PyPi packages on cloud composer for deployment)
- Tableau Cloud account with PAT access and access to the workbook or datasource to be refreshed.
- Airflow connection set up with Tableau credentials (see below)

---

## Airflow Connection Setup

Connection should be created in **GCP Secret Manager** so that you can use secrets with your Cloud Composer environment. This ensure your Airflow connection secrets are securely stored.

To store your Tableau connection details in Secret Manager, you must use a URI representation as shown below:

```html
http://https://<Host>/?site_id=<your-site-id>&token_name=<your-pat-name>&personal_access_token=<your-pat-secret>
```

Where
- **Host** is the URL of your Tableau cloud without subpaths e.g `https://eu-west-1a.online.tableau.com`
- **site_id** is your site id e.g `mysite`
- **token_name** is your Personal Access Token name.
- **personal_access_token** is the value of your Personal Access Token.

Replace the placeholders in the url with actual values.

### Steps to add connections in Secret Manager
- Create a secret in Secret Manager:
  - Your secret name must use the `[connection_prefix][sep][connection_name]` format.
  - The default value for `[connection_prefix]` is `airflow-connections`.
  - The default separator `[sep]` is `-`.
For example, if the connection name is `exampleConnection`, then the secret name is `airflow-connections-exampleConnection`.

- Store the url created above as the secret value.

For more information on how to configure Secret Manager for Composer environment, check the docs [here](https://cloud.google.com/composer/docs/composer-3/configure-secret-manager#configure).

---

## File Placement

Place the custom operator and hook Python files in your Airflow DAGs directory under a folder like `includes/`.

```
dags/
├── includes/
│   ├── tableau_hook.py  # Contains TableauAPIHook
│   └── tableau_operator.py  # Contains TableauOperator
├── example_dag.py # Your DAG file
```

---

## How to Use

### Import and Example DAG Task

```python
from includes.tableau_operator import TableauApiOperator

refresh_workbook = TableauApiOperator(
    task_id="refresh_tableau_workbook_blocking",
    resource="workbooks",  # Argument can either be workbooks or "datasources"
    method="refresh",  # default
    resource_name="Sales Dashboard",
    project_name="Marketing",
    blocking_refresh=True,
    tableau_conn_id="tableau_default"  # default
)
```

---

## Operator Parameters

| Parameter         | Type    | Required | Description |
|------------------|---------|----------|-------------|
| `resource`        | `str`   | ✅        | `"workbooks"` or `"datasources"` |
| `method`          | `str`   | ❌        | Method to perform (default: `"refresh"`) |
| `resource_name`   | `str`   | ✅        | Name of the workbook or datasource |
| `project_name`    | `str`   | ✅        | Name of the Tableau project the resource belongs to |
| `blocking_refresh`| `bool`  | ❌        | Wait for refresh to complete (default: `True`) |
| `tableau_conn_id` | `str`   | ✅        | Airflow connection ID (default: `"tableau_default"`) |

---

## Notes
- You can extend this operator to support other Tableau API features if needed.
- Avoid hardcoding PAT credentials — always use GCP secrets manager.

---

## TODO

There is an ongoing work to make this provider easier to install on local environment and from a private python package repository.

---
