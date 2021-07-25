import psycopg2


#establishing the connection
conn = psycopg2.connect(
   database="nszu", user='nataliia', password='legion', host='127.0.0.1', port='5432'
)

cur = conn.cursor()
cur.execute('SELECT e.edrpou '
            'FROM events e '
            'INNER JOIN injured_areas ia ON ia.id = e.id '
            "WHERE (e.inserted_at like '%-11-%' or e.inserted_at like '%-12-%') and ia.count_inj > 1"
            'GROUP BY e.edrpou '
            'HAVING COUNT(e.id) > 4;')
print(cur.fetchall())



