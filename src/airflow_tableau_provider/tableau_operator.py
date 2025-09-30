"""Airflow Operator for Tableau.

This custom Airflow operator allows seamless triggering of Tableau Cloud
workbook or datasource extract refreshes immediately after a data pipeline run
is complete, ensuring dashboards are always in sync with data pipeline runs.

Author: Najeeb Sulaiman
"""

import logging

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from tableauserverclient import ServerResponseError

from airflow_tableau_provider.tableau_hook import TableauApiHook

# Dictionary to specify which resources and methods are available.
available_resources = {
    "datasources": "refresh",
    "workbooks": "refresh"
}


class TableauApiOperator(BaseOperator):
    """Tableau operator to execute a Tableau API resource."""

    def __init__(  # noqa: PLR0913
            self,
            *,
            resource: str,
            method: str = "refresh",
            resource_name: str,
            project_name: str,
            site_id: str | None = None,
            blocking_refresh: bool = True,
            tableau_conn_id: str = "tableau_default",
            **kwargs,
    ) -> None:
        """Initialize the class with Tableau resource configuration.

        Args:
        resource (str): Name of the tableau resource to use (e.g workbooks).
        method (str): Name of the resource's method to execute (e.g refresh).
        resource_name (str): Name of the resource that will receive the action.
        project_name (str): Name of the Tableau project.
        site_id (str): The id of the site where the resource belongs to.
        blocking_refresh(bool): Wait for job completion or not (Default - True)
        tableau_conn_id (str): The Tableau connection id setup on Airflow UI
        (Default is "tableau_default").
        **kwargs: Additional keyword arguments.
        """
        super().__init__(**kwargs)
        self.resource = resource
        self.method = method
        self.resource_name = resource_name
        self.project_name = project_name
        self.site_id = site_id
        self.blocking_refresh = blocking_refresh
        self.tableau_conn_id = tableau_conn_id

    def get_resource_id(self, tableau_hook: TableauApiHook,
                        project_name: str,
                        resource_name: str):
        """Get the resource id of the specified resource.

        Args:
            tableau_hook (class): Tableau hook for establishing connection.
            project_name (str): The resource's project name.
            resource_name (str): The name of resource that will recieve action.

        Returns:
            workbook id
        """
    # Filter all the resources on the server to get the matching resource
        resource = getattr(tableau_hook.server, self.resource)
        get_resource = resource.filter(
            project_name=project_name,
            name=resource_name)

        # Get the resource id of the macthing resource.
        resource_id = get_resource[0].id
        logging.info(
            "Here is the resource id for the %s specified: %s",
            self.resource,
            resource_id)

        return resource_id

    def execute(self, context: Context) -> str:
        """Execute action on the Tableau API resource.

        Args:
            context (Context) : The task context during execution.

        Returns:
            The id of the job that executes the Tableau API action.
        """
        # Check if specified resource and method exist.
        if self.resource not in available_resources:
            error_message = (
                "Resource not found! "
                f"Available Resources: {available_resources}"
              )
            raise AirflowException(error_message)

        available_methods = available_resources[self.resource]
        if self.method not in available_methods:
            error_message = (
                "Method not found! "
                f"Available methods for {self.resource}: {available_methods}"
              )
            raise AirflowException(error_message)

        with TableauApiHook(self.site_id,
                            self.tableau_conn_id) as tableau_hook:

            # Get resource id to perform action on.
            resource_id = self.get_resource_id(
                tableau_hook,
                self.project_name,
                self.resource_name)

            try:
                # Trigger action on a specified resource by calling Tableau API
                # functions on the server object. This is similar to doing:
                # job = server.workbooks.refresh(workbook_id)  # noqa: ERA001
                # for an example workbook refresh.
                resource = getattr(tableau_hook.server, self.resource)
                method = getattr(resource, self.method)

                # Job info is returned and stored in the job variable.
                job = method(resource_id)
                logging.info("\nThe refresh of %s: %s has been triggered.",
                             self.resource, resource_id)

                job_id = job.id
            except ServerResponseError as e:
                logging.error("Refresh failed! %s", e)
                raise

            # Wait for job or not if the action performed is "refresh".
            if self.method == "refresh" and self.blocking_refresh:
                self.wait_for_job(tableau_hook, job_id)

        return job_id

    def wait_for_job(self, tableau_hook: TableauApiHook, job_id: str):
        """Waits until a given job succesfully finishes.

        Repeatedly polls the server using jobs.get_by_id
        until the job completed. It uses an exponentially
        increasing sleep time between polls to guarantee
        a snappy response time for fast-running jobs while
        not putting overly large load on the server for long-running jobs.

        Args:
            tableau_hook (class): Tableau hook for establishing connection.
            job_id (str): id of the job that executes the Tableau API action.

        Performs:
            wait for refresh job to complete.
        """
    # `wait_for_job` throws exception if the job isn't executed successfully
        try:
            logging.info("Waiting for job...")
            # Wait until a given job succesfully finishes
            # wait_for_job is available from tableauserverclient
            tableau_hook.server.jobs.wait_for_job(job_id)
            logging.info("Job finished succesfully")
        except ServerResponseError as e:
            logging.error("Job failed! %s", e)
            raise
