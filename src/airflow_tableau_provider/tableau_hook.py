"""Airflow hook for Tableau.

This custom Airflow hook enables connection to Tableau cloud
via personal access token.

Author: Najeeb Sulaiman
"""

from typing import Any

from airflow.hooks.base import BaseHook
from tableauserverclient import PersonalAccessTokenAuth, Server
from tableauserverclient.server import Auth


class TableauApiHook(BaseHook):
    """Establish connection with the Tableau Server and allows communication.

    Can be used as a context manager: automatically closes connection after use

    Args:
        site_id (str): The id of the site where the resource belongs to.
        tableau_conn_id (str): The Tableau connection id setup on Airflow UI
        (Default is "tableau_default").
    """

    # Default connection id.
    default_conn_name = "tableau_default"

    def __init__(self,
                 site_id: str | None = None,
                 tableau_conn_id: str = default_conn_name) -> None:
        """The class constructor."""
        super().__init__()

        self.tableau_conn_id = tableau_conn_id

        self.conn = self.get_connection(self.tableau_conn_id)

        self.site_id = site_id or self.conn.extra_dejson.get("site_id", "")

        self.server = Server(self.conn.host, use_server_version=True)

    def __enter__(self):
        """Opens connection when using context manager."""
        self.tableau_conn = self.get_conn()

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        """Closes connection when using context manager."""
        self.server.auth.sign_out()

    def get_conn(self) -> Auth.contextmgr:
        """Sign in to the Tableau Server.

        returns: an authorized Tableau Server Context Manager object.
        """
        tableau_auth = PersonalAccessTokenAuth(
            self.conn.extra_dejson["token_name"],
            self.conn.extra_dejson["personal_access_token"],
            self.site_id,
        )

        return self.server.auth.sign_in_with_personal_access_token(
            tableau_auth
            )
