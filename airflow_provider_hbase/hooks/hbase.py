from airflow.hooks.base import BaseHook
from typing import Any, Dict, Optional
import happybase

class HBaseHook(BaseHook):
    
    conn_name_attr = "hbase_conn_id"
    default_conn_name = "hbase_default"
    conn_type = "hbase"
    hook_name = "Hbase"
    
    @classmethod
    def get_connection_form_widgets(cls) -> Dict[str, Any]:
        """ui에 표시될 추가 정보 필드를 생성한다."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "transport": StringField(lazy_gettext("Transport"), widget=BS3TextFieldWidget()),
            "protocol": StringField(lazy_gettext("Protocol"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        """Returns custom field behaviour"""
        import json
        
        return {
            "hidden_fields": ["password", "login", "schema"],
            "relabeling": {},
            "placeholders": {
                "host": "localhost",
                "port": "9090", # placeholders는 문자열로 선언해야 한다.
                "transport": "buffered",
                "protocol": "binary",
            },
        }
        
    def __init__(
        self, 
        *, # 위치 기반 인수를 허용하지 않음
        hbase_conn_id: str = "hbase_default",
        host: Optional[str] = None,
        port: Optional[int] = None,
        timeout: Optional[int] = None,
        autoconnect: Optional[bool] = None,
        table_prefix: Optional[str] = None,
        table_prefix_separator: Optional[str] = None,
        compat: Optional[str] = None,
        transport: Optional[str] = None,
        protocol: Optional[str] = None,
        **kwargs: Any
    ):
        self.hbase_conn_id = hbase_conn_id
        self.host = host
        self.port = port
        self.timeout = timeout
        self.autoconnect = autoconnect
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat
        self.transport = transport
        self.protocol = protocol
        self.kwargs = kwargs

    def get_conn(self) -> happybase.Connection:
        connection = self.get_connection(self.hbase_conn_id)
        conn_params = {
            'host': connection.host,
            'port': connection.port,
            'timeout': connection.extra_dejson.get('timeout', None),
            'autoconnect': connection.extra_dejson.get('autoconnect', True),
            'table_prefix': connection.extra_dejson.get('table_prefix', None),
            'table_prefix_separator': connection.extra_dejson.get('table_prefix_separator', b'_'),
            'compat': connection.extra_dejson.get('compat', '0.98'),
            'transport': connection.extra_dejson.get('transport', 'buffered'),
            'protocol': connection.extra_dejson.get('protocol', 'binary')
        }

        # 사용자가 제공한 값으로 override
        for param, value in {
            'host': self.host,
            'port': self.port,
            'timeout': self.timeout,
            'autoconnect': self.autoconnect,
            'table_prefix': self.table_prefix,
            'table_prefix_separator': self.table_prefix_separator,
            'compat': self.compat,
            'transport': self.transport,
            'protocol': self.protocol
        }.items():
            if value is not None:
                conn_params[param] = value

        # 추가 kwargs 업데이트
        conn_params.update(self.kwargs)

        return happybase.Connection(**conn_params)
