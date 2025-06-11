from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.utils.session import create_session
from airflow.utils.types import ConnectionType
from airflow.utils.db import provide_session
from typing import Optional

class CDPConnection(BaseHook):
    """
    CDP connection type for handling access_id and access_key credentials.
    """
    conn_name_attr = 'cdp_conn_id'
    default_conn_name = 'cdp_default'
    conn_type = 'cdp'
    hook_name = 'CDP'

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns custom widgets to be added for the hook to handle extra fields."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "access_id": StringField(
                lazy_gettext('Access ID'),
                widget=BS3TextFieldWidget()
            ),
            "access_key": StringField(
                lazy_gettext('Access Key'),
                widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behavior."""
        return {
            "hidden_fields": ['port', 'schema', 'login', 'password', 'host', 'extra'],
            "relabeling": {},
            "placeholders": {
                'access_id': 'Enter your access ID',
                'access_key': 'Enter your access key',
            },
        }

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__()
        self.conn_id = conn_id
        self.conn = None

    def get_conn(self) -> Connection:
        """Returns the connection object."""
        if not self.conn:
            self.conn = self.get_connection(self.conn_id)
        return self.conn

    def get_access_id(self) -> str:
        """Returns the access ID from the connection."""
        return self.get_conn().extra_dejson.get('access_id')

    def get_access_key(self) -> str:
        """Returns the access key from the connection."""
        return self.get_conn().extra_dejson.get('access_key') 