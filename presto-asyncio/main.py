import prestodb


if __name__ == "__main__":
    conn = prestodb.dbapi.connect(
        host='host',
        port='443',
        user='id',
        catalog='catalog',
        schema='schema',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication("id", "pwd"),
    )

    cur = conn.cursor()
    cur.execute('SELECT * FROM persons')
    rows = cur.fetchall()
    for row in rows:
        print(row)