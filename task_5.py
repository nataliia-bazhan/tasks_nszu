import psycopg2

#establishing the connection
conn = psycopg2.connect(
   database="nszu", user='nataliia', password='legion', host='127.0.0.1', port='5432'
)

cur = conn.cursor()
cur.execute('UPDATE events SET payment = '
            'CASE '
            'WHEN events.packet_number = 9 THEN (SELECT p.base_rate FROM packets p WHERE p.packet = events.packet_number) '
            'ELSE (SELECT p.base_rate FROM packets p WHERE p.packet = events.packet_number)*(SELECT s.coef FROM services s WHERE s.service = events.service_number) '
            'END;')
conn.commit()
cur.execute('select * from events;')
l = cur.fetchall()
for i in l:
   print(i)
