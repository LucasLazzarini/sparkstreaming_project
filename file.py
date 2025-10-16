# Databricks notebook source
"""Connection with Fivetran APIs.

This module provides a class to interact with Fivetran connectors,
allowing to trigger syncs, monitor sync status, and pause/unpause connectors.
"""

import requests
import time
import base64
import json
from requests.auth import HTTPBasicAuth

class FivetranAPI:
    """Interface for interacting with the Fivetran API for a specific connector."""

    def __init__(self, CONNECTOR_NAME: str) -> None:
        """Initializes the FivetranAPI class with the given connector name.

        Retrieves API credentials from the Databricks secret scope.

        Args:
            CONNECTOR_NAME (str): The unique name (ID) of the Fivetran connector.
        """
        self.URL = "https://api.fivetran.com/v1"
        self.API_KEY = dbutils.secrets.get(scope="fivetran_bi_service", key="api_key")
        self.API_SECRET = dbutils.secrets.get(scope="fivetran_bi_service", key="api_secret")
        self.CONNECTOR_NAME = {
            "sherwood_palsql": "tidy_interval",
            "fsq_labworks": "intensive_usefulness",
            "hubspot": "embarkation_cropped",
            "tulare_soiltestvtaglab": "shucking_fresco",
            "netsuite_suiteanalytics": "decayed_neat"
        }[CONNECTOR_NAME]

    def __call__(self):
        """Triggers a sync, monitors its status until completion, and pauses the connector.

        This method:
        - Unpauses the connector if it is paused
        - Waits 15 seconds before starting to monitor
        - Continuously checks sync status every 30 seconds
        - Repauses the connector when sync completes
        - Prints logs along the process
        """
        auth_header = self.get_auth_header()
        if self.get_sync_status(headers=auth_header) == 'paused':
            self.set_connector_paused_status(status_paused=False)

        print("Waiting 15 seconds to start the monitoring!")
        time.sleep(15)

        while True:
            status = self.get_sync_status(headers=auth_header)
            print(f"Sync Status: {status}")
            if status not in ["syncing"]:
                print(f"Sync ended -> status: {status}")
                break
            time.sleep(30)

        print("Repausing Sync!")
        self.set_connector_paused_status(status_paused=True)

        print("Start your pipeline!")

    def get_auth_header(self) -> json:
        """Generates the Basic Auth header using the API key and secret.

        Returns:
            dict: Authorization header with Basic token.
        """
        credentials = f"{self.API_KEY}:{self.API_SECRET}"
        token = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def get_sync_status(self, headers: json) -> str:
        """Retrieves the current sync status of the connector.

        Args:
            headers (dict): Authorization header.

        Returns:
            str: Current sync status. Possible values include:
                 'syncing', 'paused', 'rescheduled', etc.
        """
        url = f"{self.URL}/connections/{self.CONNECTOR_NAME}"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        sync_state = resp.json()["data"]["status"]["sync_state"]
        return sync_state

    def set_connector_paused_status(self, status_paused: bool) -> None:
        """Pauses or unpauses the Fivetran connector.

        Args:
            status_paused (bool): If True, pauses the connector. If False, unpauses it.

        Logs:
            Prints success or error messages based on the response.
        """
        url = f'{self.URL}/connectors/{self.CONNECTOR_NAME}'
        headers = {'Content-Type': 'application/json'}
        payload = {'paused': status_paused}

        response = requests.patch(
            url,
            auth=HTTPBasicAuth(self.API_KEY, self.API_SECRET),
            headers=headers,
            json=payload
        )

        if response.status_code == 200:
            status = "PAUSED" if status_paused else "UNPAUSED"
            print(f"Connector {self.CONNECTOR_NAME} was successfully {status}.")
        else:
            print(f"Error updating the connector: {response.status_code}")
            print(response.json())


if __name__ == "__main__":
    FivetranAPI(CONNECTOR_NAME=dbutils.widgets.get("connector_name"))()
