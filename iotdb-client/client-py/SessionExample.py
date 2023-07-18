from iotdb.dbapi import connect
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
conn = connect(ip, port_, username_, password_)
cursor = conn.cursor()
cursor.execute("SELECT ** FROM root")
for row in cursor.fetchall():
    print(row)
cursor.close()
conn.close()
