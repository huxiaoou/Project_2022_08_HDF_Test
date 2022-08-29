from setup import *


@timer
def test_to_csv(t_df: pd.DataFrame, t_save_dir: str,
                t_calendar: CCalendar, t_bgn_date: str, t_stp_date: str):
    check_and_mkdir(t_save_dir)
    for trade_date in t_calendar.get_iter_list(t_bgn_date=t_bgn_date, t_stp_date=t_stp_date, t_ascending=True):
        check_and_mkdir(os.path.join(t_save_dir, trade_date[0:4]))
        check_and_mkdir(os.path.join(t_save_dir, trade_date[0:4], trade_date))
        t_save_path = os.path.join(t_save_dir, trade_date[0:4], trade_date, "{}.csv.gz".format(trade_date))
        t_df.to_csv(t_save_path, index=False, float_format="%.6f")
    return 0


@timer
def test_to_hdf5(t_df: pd.DataFrame, t_save_dir: str,
                 t_calendar: CCalendar, t_bgn_date: str, t_stp_date: str):
    check_and_mkdir(t_save_dir)
    for trade_date in t_calendar.get_iter_list(t_bgn_date=t_bgn_date, t_stp_date=t_stp_date, t_ascending=True):
        # h5_file = pd.HDFStore(os.path.join(t_save_dir, "example.h5"))
        # h5_file.put(key="example", value=t_df)
        # t_df = h5_file.get("example")
        # h5_file.close()
        t_save_path = os.path.join(t_save_dir, "example.h5")
        t_df.to_hdf(t_save_path, key="D" + trade_date, mode="a")
    return 0


@timer
def test_to_db(t_df: pd.DataFrame, t_save_dir: str,
               t_calendar: CCalendar, t_bgn_date: str, t_stp_date: str):
    check_and_mkdir(t_save_dir)

    # check db path
    db_path = os.path.join(t_save_dir, "factors.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    # create database
    connection = sql3.connect(db_path)
    cursor = connection.cursor()
    col_names = t_df["instrument"].tolist()

    # create table
    cmd_sql_for_create_table = "CREATE table DAILY_FACTORS ({}, {})".format(
        "trade_date TEXT PRIMARY KEY",
        ", ".join(["{} REAL".format(z.replace(".", "_")) for z in col_names])
    )
    cursor.execute(cmd_sql_for_create_table)

    for trade_date in t_calendar.get_iter_list(t_bgn_date=t_bgn_date, t_stp_date=t_stp_date, t_ascending=True):
        data = [trade_date] + t_df["factor"].tolist()
        cursor.execute("INSERT INTO DAILY_FACTORS VALUES ({}, {})".format("?", ", ".join(["?"] * len(t_df))), data)
    connection.commit()
    cursor.close()
    connection.close()
    return 0


@timer
def test_read_csv(t_save_dir: str,
                  t_calendar: CCalendar, t_bgn_date: str, t_stp_date: str):
    for trade_date in t_calendar.get_iter_list(t_bgn_date=t_bgn_date, t_stp_date=t_stp_date, t_ascending=True):
        t_save_path = os.path.join(t_save_dir, trade_date[0:4], trade_date, "{}.csv.gz".format(trade_date))
        t_df = pd.read_csv(t_save_path)
    return 0


@timer
def test_read_hdf5(t_save_dir: str,
                   t_calendar: CCalendar, t_bgn_date: str, t_stp_date: str):
    t_save_path = os.path.join(t_save_dir, "example.h5")
    hdf = pd.HDFStore(t_save_path, mode="r")
    for trade_date in t_calendar.get_iter_list(t_bgn_date=t_bgn_date, t_stp_date=t_stp_date, t_ascending=True):
        t_df = hdf.get("/D{}".format(trade_date))
    hdf.close()
    return 0


@timer
def test_read_db(t_save_dir: str,
                 t_calendar: CCalendar, t_bgn_date: str, t_stp_date: str):
    # check db path
    db_path = os.path.join(t_save_dir, "factors.db")

    # create database
    connection = sql3.connect(db_path)
    cursor = connection.cursor()
    table_info = cursor.execute("PRAGMA table_info(DAILY_FACTORS)").fetchall()
    col_names = [z[1] for z in table_info][1:]
    cmd_sql_for_inquiry = "SELECT {} FROM DAILY_FACTORS where trade_date = ".format(", ".join(col_names))

    for trade_date in t_calendar.get_iter_list(t_bgn_date=t_bgn_date, t_stp_date=t_stp_date, t_ascending=True):
        rows = cursor.execute("{}{}".format(cmd_sql_for_inquiry, trade_date)).fetchall()
        t_df = pd.DataFrame(data=rows, columns=col_names, index=["factor"]).T
    cursor.close()
    connection.close()
    return 0


instrument_list = [
    "AU.SHF",
    "AG.SHF",
    "CU.SHF",
    "AL.SHF",
    "PB.SHF",
    "ZN.SHF",
    "SN.SHF",
    "NI.SHF",
    "SS.SHF",
    "RB.SHF",
    "HC.SHF",
]
list_size, k = len(instrument_list), 1
df = pd.DataFrame({
    "instrument": instrument_list * k,
    "factor": np.random.random(size=list_size * k)
})

print(df)
print("\n" * 2)

cne_calendar = CCalendar(t_path=SKYRIM_CONST_CALENDAR_PATH)

bgn_date = "20150101"
stp_date = "20220825"

# test write
test_to_csv(t_df=df, t_save_dir=project_data_dir, t_calendar=cne_calendar, t_bgn_date=bgn_date, t_stp_date=stp_date)
# test_to_hdf5(t_df=df, t_save_dir=project_data_dir, t_calendar=cne_calendar, t_bgn_date=bgn_date, t_stp_date=stp_date)
test_to_db(t_df=df, t_save_dir=project_data_dir, t_calendar=cne_calendar, t_bgn_date=bgn_date, t_stp_date=stp_date)

# test read
test_read_csv(t_save_dir=project_data_dir, t_calendar=cne_calendar, t_bgn_date=bgn_date, t_stp_date=stp_date)
# test_read_hdf5(t_save_dir=project_data_dir, t_calendar=cne_calendar, t_bgn_date=bgn_date, t_stp_date=stp_date)
test_read_db(t_save_dir=project_data_dir, t_calendar=cne_calendar, t_bgn_date=bgn_date, t_stp_date=stp_date)
