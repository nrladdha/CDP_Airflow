from __future__ import annotations
from typing import Optional, List, Dict, Any
import subprocess
import time
import warnings
import json
from airflow.models import BaseOperator
#from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from .connections import CDPConnection


class CDPDataHubOperator(BaseOperator):
    """
    Airflow operator to manage CDP DataHub clusters.
    
    This operator can:
    1. Start a CDP DataHub cluster
    2. Stop the cluster
    
    :param cluster_name: Name of the CDP DataHub cluster
    :type cluster_name: str
    :param environment_name: CDP environment name
    :type environment_name: str
    :param wait_for_cluster: Whether to wait for cluster to be ready before proceeding
    :type wait_for_cluster: bool
    :param cluster_wait_timeout: Timeout in seconds for waiting cluster to be ready
    :type cluster_wait_timeout: int
    :param operation: The operation to perform ('start' or 'stop')
    :type operation: str
    :param cdp_conn_id: The connection ID to use for credentials
    :type cdp_conn_id: str
    """

    #@apply_defaults
    def __init__(
        self,
        cluster_name: str,
        environment_name: str,
        operation: str,
        wait_for_cluster: bool = True,
        cluster_wait_timeout: int = 1800,
        cdp_conn_id: str = 'cdp_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_name = cluster_name
        self.environment_name = environment_name
        self.operation = operation.lower()
        self.wait_for_cluster = wait_for_cluster
        self.cluster_wait_timeout = cluster_wait_timeout
        self.cdp_conn_id = cdp_conn_id
        self.start_result = None
        self.stop_result = None
        
        if self.operation not in ['start', 'stop']:
            raise ValueError("Operation must be either 'start' or 'stop'")

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the operator's tasks.
        """
        try:
            # Get credentials from CDP connection
            cdp_conn = CDPConnection(conn_id=self.cdp_conn_id)
            access_id = cdp_conn.get_access_id()
            access_key = cdp_conn.get_access_key()

            if not access_id or not access_key:
                raise AirflowException("Access ID and Access Key must be provided in the CDP connection")

            # Set environment variables for CDP CLI
            self.log.info(f"Input:cluster_name: {self.cluster_name}")
            self.log.info(f"Input:environment_name: {self.environment_name}")
            self.log.info(f"Input:operation: {self.operation}")
            self.log.info(f"Input:wait_for_cluster: {self.wait_for_cluster}")
            self.log.info(f"Input:cluster_wait_timeout: {self.cluster_wait_timeout}")

            # Set credentials in environment
            import os
            os.environ['CDP_ACCESS_KEY_ID'] = access_id
            os.environ['CDP_PRIVATE_KEY'] = access_key

            if self.operation == 'start':
                self.log.info(f"Starting CDP DataHub cluster: {self.cluster_name}")
                self._start_cluster()
                if self.wait_for_cluster:
                    self._wait_for_cluster_ready()
            elif self.operation == 'stop':
                self.log.info(f"Stopping CDP DataHub cluster: {self.cluster_name}")
                self._stop_cluster()
                if self.wait_for_cluster:
                    self._wait_for_cluster_ready()
            else:
                self.log.info(f"CDP DataHub cluster - Invalid operation supplied : {self.operation}")
            
        except Exception as e:
            self.log.error(f"Error in CDPDataHubOperator: {str(e)}")
            raise AirflowException(f"CDPDataHubOperator failed: {str(e)}")

    def _start_cluster(self) -> None:
        """Start the CDP DataHub cluster."""
        cmd = [
            "cdp", "datahub", "start-cluster",
            "--cluster-name", self.cluster_name
        ]
        start_result=self._run_command(cmd, capture_output=True)

    def _stop_cluster(self) -> None:
        """Stop the CDP DataHub cluster."""
        cmd = [
            "cdp", "datahub", "stop-cluster",
            "--cluster-name", self.cluster_name
        ]
        stop_result=self._run_command(cmd, capture_output=True)

    def _wait_for_cluster_ready(self) -> None:
        """Wait for the cluster to be ready."""
        start_time = time.time()
        while time.time() - start_time < self.cluster_wait_timeout:
            cmd = [
                "cdp", "datahub", "describe-cluster",
                "--cluster-name", self.cluster_name
            ]
            wait_for_result = self._run_command(cmd, capture_output=True)

            # Parse stdout as JSON
            output_json = json.loads(wait_for_result.stdout)
            operation_status = output_json["cluster"]["clusterStatus"]
           
            if self.operation == 'start':
                if "AVAILABLE" in operation_status:
                    self.log.info("Cluster is running")
                    return
            elif self.operation == 'stop':
                if "STOPPED" in operation_status:
                    self.log.info("Cluster is stopped")
                    return
            
            self.log.info("Waiting for cluster operation to finish...")
            time.sleep(30)
        
        raise AirflowException(f"Timeout occured : {self.cluster_wait_timeout} : Cluster operation could not finish")

    def _run_command(self, cmd: List[str], capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a shell command and return the result."""
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=capture_output,
                text=True
            )
            return result
        except subprocess.CalledProcessError as e:
            raise AirflowException(f"Command failed: {e.stderr}") 