import psycopg2

#establishing the connection
conn = psycopg2.connect(
   database="nszu", user='nataliia', password='legion', host='127.0.0.1', port='5432'
)

cur = conn.cursor()
#cur.execute('select * from contracts;')
cur.execute('select c1.edrpou, c1.month, c1.budget_limit, c1.packet, SUM(c2.budget_limit) as budget_sum '
            'from contracts c1 '
            'inner join contracts c2 on '
            '(c1.edrpou = c2.edrpou and c1.month >= c2.month and c1.packet = c2.packet) '
            'group by c1.edrpou, c1.month, c1.budget_limit, c1.packet '
            #'having c1.edrpou = 34636246 and c1.packet=2 '
            'order by c1.edrpou;')
l = cur.fetchall()
for i in l:
   print(i)
