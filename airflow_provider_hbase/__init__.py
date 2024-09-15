def get_provider_info():
    return {
        "package-name": "airflow-provider-hbase",
        "name": "Hbase",
        "description": "apache airflow custom hbase provider",
        "connection-types": [
            {
                "connection-type": "hbase",
                "hook-class-name": "airflow_provider_hbase.hooks.hbase.HBaseHook"
            }
        ],
    }