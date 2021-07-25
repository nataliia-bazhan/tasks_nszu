import psycopg2
import pandas as pd

def fill_events(conn):
    #print('starting')
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('events',))
    exist = cur.fetchone()[0]
    if exist:
        #print('dropping')
        cur.execute('drop table events;')
    #print('creating')
    cur.execute('create table events ('
                'id char(36),'
                'edrpou int,'
                'inserted_at char(23),'
                'icd_codes varchar(200),'
                'discharge_code varchar(90),'
                'packet_number float,'
                'service_number varchar(30),'
                'payment float);')
    data_events = pd.read_csv('events.csv')
    for i in range(len(data_events)):
        row = list(data_events.iloc[i, :])
        #print(row)
        row[1] = int(row[1])
        cur.execute("INSERT INTO events VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", row)
    conn.commit()

def fill_injured_areas(conn):
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('injured_areas',))
    exist = cur.fetchone()[0]
    if exist:
        cur.execute('drop table injured_areas;')
    cur.execute('create table injured_areas ('
                'id char(36),'
                'count_inj float);')
    data_injured_areas = pd.read_csv('injured_areas.csv')
    for i in range(len(data_injured_areas)):
        row = list(data_injured_areas.iloc[i, 1:])
        cur.execute("INSERT INTO injured_areas VALUES (%s, %s);", row)
    conn.commit()

def fill_contracts(conn):
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('contracts',))
    exist = cur.fetchone()[0]
    if exist:
        cur.execute('drop table contracts;')
    cur.execute('create table contracts ('
                'edrpou float,'
                'month int,'
                'budget_limit float,'
                'packet int);')
    contracts = pd.read_csv('contracts.csv')
    for i in range(len(contracts)):
        row = list(contracts.iloc[i, :])
        cur.execute("INSERT INTO contracts VALUES (%s, %s, %s, %s);", row)
    conn.commit()

def fill_packets(conn):
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('packets',))
    exist = cur.fetchone()[0]
    if exist:
        cur.execute('drop table packets;')
    cur.execute('create table packets ('
                'packet int,'
                'base_rate float,'
                'name varchar(400));')
    packets = pd.read_csv('packets.csv')
    for i in range(len(packets)):
        row = list(packets.iloc[i, :])
        row[0] = int(row[0])
        row[1] = float(row[1])
        cur.execute("INSERT INTO packets VALUES (%s, %s, %s);", row)
    conn.commit()

def fill_services(conn):
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('services',))
    exist = cur.fetchone()[0]
    if exist:
        cur.execute('drop table services;')
    cur.execute('create table services ('
                'packet int,'
                'service varchar(20),'
                'coef float,'
                'name varchar(400));')
    services = pd.read_csv('services.csv')
    for i in range(len(services)):
        row = list(services.iloc[i, :])
        row[0] = int(row[0])
        row[2] = float(row[2])
        cur.execute("INSERT INTO services VALUES (%s, %s, %s, %s);", row)
    conn.commit()

def fill_small(conn):
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('small',))
    exist = cur.fetchone()[0]
    if exist:
        cur.execute('drop table small;')
    cur.execute('create table small ('
                'id char(36),'
                'edrpou int,'
                'inserted_at char(23),'
                'icd_codes varchar(200),'
                'discharge_code varchar(90),'
                'packet_number float,'
                'service_number varchar(30),'
                'payment float);')
    data_events = pd.read_csv('small(1).csv')
    for i in range(len(data_events)):
        row = list(data_events.iloc[i, :])
        #print(row)
        row[0] = int(row[0])
        row[2] = int(row[2])
        #row[5] = float(row[5])
        #row[7] = float(row[7])
        cur.execute("INSERT INTO small VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", row[1:])
    conn.commit()

#establishing the connection
conn = psycopg2.connect(
   database="nszu", user='nataliia', password='legion', host='127.0.0.1', port='5432'
)

#fill_injured_areas(conn)
#fill_events(conn)
fill_contracts(conn)
#fill_packets(conn)
#fill_services(conn)
#fill_small(conn)