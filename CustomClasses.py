import pandas as pd
import sqlite3 as sql3


class CFactorsLib(object):
    def __init__(self, t_db_path: str):
        self._db_path: str = t_db_path
        self._connection = sql3.connect(self._db_path)
        self._cursor = self._connection.cursor()

    def add_factor_table(self, t_factor: str):
        cmd_sql_for_create_table = "CREATE TABLE IF NOT EXISTS {}({}, {}, {}, {})".format(
            t_factor.upper(),
            "trade_date TEXT",
            "instrument TEXT",
            "value REAL",
            "PRIMARY KEY(trade_date, instrument)"
        )
        self._cursor.execute(cmd_sql_for_create_table)
        # print("... Add {} to {} as a new table".format(t_factor, self._db_path))
        return 0

    def update(self, t_factor: str, t_trade_date: str, t_factors_df: pd.DataFrame):
        cmd_dql_for_update = "INSERT OR REPLACE INTO {} (trade_date, instrument, value) values(?, ?, ?)".format(t_factor)
        for instrument, factor_value in zip(t_factors_df.index, t_factors_df[t_factor]):
            # cur.execute("INSERT INTO myTable(id,name) VALUES(?,?)", ("1", "张三丰"))
            self._cursor.execute(cmd_dql_for_update, (t_trade_date, instrument, factor_value))
        return 0

    def read_by_date(self, t_factor: str, t_trade_date: str):
        cmd_sql_for_inquiry = "SELECT instrument, value FROM {} where trade_date = {}".format(t_factor, t_trade_date)
        rows = self._cursor.execute(cmd_sql_for_inquiry).fetchall()
        t_df = pd.DataFrame(data=rows, columns=[["instrument", "factor"]]).set_index("instrument")
        return 0

    def close(self):
        self._connection.commit()
        self._cursor.close()
        self._connection.close()
        return 0
