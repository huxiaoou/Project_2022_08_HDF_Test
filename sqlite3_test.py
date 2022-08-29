from setup import *

col_names = ["trade_date", "AU_SHF", "D_DCE", "OI_CZC"]
data_list = [
    ("20220827", -1, 0, 1),
    ("20220828", -2, 0, None),
    ("20220829", -3, 0, -1),
    ("20220830", -1, None, -1),
    ("20220831", None, 0, 1),
]

db_path = os.path.join(project_data_dir, "factors.db")

if os.path.exists(db_path):
    os.remove(db_path)

connection = sql3.connect(db_path)
cursor = connection.cursor()
cursor.execute("CREATE table DAILY_FACTORS ({} TEXT, {} DOUBLE, {} REAL, {} REAL)".format(
    col_names[0], col_names[1], col_names[2], col_names[3]))

for data in data_list:
    cursor.execute("INSERT INTO DAILY_FACTORS VALUES (?, ?, ?, ?)", data)
connection.commit()

# fetch all
rows = cursor.execute("SELECT * FROM DAILY_FACTORS").fetchall()
df = pd.DataFrame(data=rows, columns=col_names)
print(df)

# fetch part
rows = cursor.execute("SELECT * FROM DAILY_FACTORS where AU_SHF <= -1 and OI_CZC <=0").fetchall()
df = pd.DataFrame(data=rows, columns=col_names)
print(df)

# fetch part
rows = cursor.execute("SELECT * FROM DAILY_FACTORS where D_DCE is not null").fetchall()
df = pd.DataFrame(data=rows, columns=col_names)
print(df)

cursor.close()
connection.close()
