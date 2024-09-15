from airflow.models.baseoperator import BaseOperator
from typing import List, Tuple
from airflow_provider_hbase.hooks.hbase import HBaseHook

class HBaseBatchPutOperator(BaseOperator):
    def __init__(
        self, 
        table_name: str, 
        data: List[Tuple[str, dict]],
        batch_size: int = 1000,
        hbase_conn_id: str = "hbase_default",
        **kwargs
        ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.data = data
        self.batch_size = batch_size
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context):
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        conn = hook.get_conn()
        table = conn.table(self.table_name)

        with table.batch(batch_size=self.batch_size) as batch:
            for row_key, data in self.data:
                batch.put(row_key, data)


class HBaseScanOperator(BaseOperator):
    def __init__(
        self, 
        table_name: str,
        hbase_conn_id: str = "hbase_default",
        scan_kwargs: dict = {},
        **kwargs
        ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.hbase_conn_id = hbase_conn_id
        self.scan_kwargs = scan_kwargs

    def execute(self, context):
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        conn = hook.get_conn()
        table = conn.table(self.table_name)
        scanned_data = list(table.scan(**self.scan_kwargs))

        for data in scanned_data:
            print(data)

        return [{"row_key": key.decode(), "data": {k.decode(): v.decode() for k, v in data.items()}} for key, data in scanned_data]
