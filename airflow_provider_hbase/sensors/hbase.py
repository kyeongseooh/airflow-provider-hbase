from typing import Optional, Sequence
from airflow.sensors.base import BaseSensorOperator
from airflow_provider_hbase.hooks.hbase import HBaseHook

class HBaseRowkeySensor(BaseSensorOperator):
    def __init__(
        self,
        *,
        table: str,
        rowkeys: Optional[Sequence[str]] = None,
        row_start: Optional[str] = None,
        row_stop: Optional[str] = None,
        hbase_conn_id: str = 'hbase_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.table = table
        self.rowkeys = rowkeys
        self.row_start = row_start
        self.row_stop = row_stop
        self.hbase_conn_id = hbase_conn_id

        if (rowkeys and (row_start or row_stop)) or (not rowkeys and not (row_start and row_stop)):
            raise ValueError("must use only rowkeys, or only row_start and row_end.")

    def poke(self, context):
        
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        conn = hook.get_conn()
        table = conn.table(self.table)

        if self.rowkeys:
            return self._check_rowkeys(table)
        else:
            return self._check_rowkey_range(table)
        
    def _check_rowkeys(self, table):
        for rowkey in self.rowkeys:
            if not table.row(rowkey):  # 하나라도 존재하지 않으면 False
                self.log.info(f"rowkey {rowkey} does not exist.")
                return False
        self.log.info("all rowkeys exist.")
        return True

    def _check_rowkey_range(self, table):
        scanner = table.scan(row_start=self.row_start, row_stop=self.row_stop, limit=1) # 조건 내에 데이터가 하나라도 있으면 True
        for key, data in scanner:
            self.log.info("rowkey exists in range.")
            return True
        self.log.info("there is no rowkey in range.")
        return False