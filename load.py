import argparse
import datetime
import psycopg

from numpy.random import permutation
from rand import NURand_CLAST, urand, alphastr, numstr, lastname

MAXITEMS = 100000
CUST_PER_DIST = 3000
DIST_PER_WARE = 10
ORD_PER_DIST = 3000
n_warehouses = 0
timestamp = datetime.datetime.now()
conn: psycopg.Connection = None

def create_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--create-tables', action='store_true')
    parser.add_argument('warehouses', type=int)
    parser.add_argument('db_connect_str', nargs='+', type=str)

    return parser.parse_args()

def load_items():
    orig = [0 for _ in range(MAXITEMS)]
    pos = -1
    i = -1

    for i in range(MAXITEMS // 10):
        pos = urand(0, MAXITEMS - 1)
        while orig[pos] == 1:
            pos = urand(0, MAXITEMS - 1)
        orig[pos] = 1
    
    for i_id in range(1, MAXITEMS + 1):
        i_name = alphastr(14, 24)
        i_price = urand(100, 10000) / 100
        i_data = alphastr(26, 50)
        idatasiz = len(i_data)

        if orig[i_id - 1] == 1:
            i_data = list(i_data)
            pos = urand(0, idatasiz - 8)
            i_data[pos+0] = 'o'
            i_data[pos+1] = 'r'
            i_data[pos+2] = 'i'
            i_data[pos+3] = 'g'
            i_data[pos+4] = 'i'
            i_data[pos+5] = 'n'
            i_data[pos+6] = 'a'
            i_data[pos+7] = 'l'
            i_data = ''.join(i_data)

        conn.execute('INSERT INTO ITEM (i_id, i_name, i_price, i_data) VALUES (%s, %s, %s, %s);', [i_id, i_name, i_price, i_data])

def load_warehouse():
    for w_id in range(1, n_warehouses + 1):
        w_name = alphastr(6, 10)
        w_street_1 = alphastr(10, 20)
        w_street_2 = alphastr(10, 20)
        w_city = alphastr(10, 20)
        w_state = alphastr(2, 2)
        w_zip = numstr(4, 4) + '11111'
        w_tax = urand(0, 2000) / 10000
        w_ytd = 300000

        conn.execute('INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', [w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd])
    
        stock(w_id)
        district(w_id)

def load_cust():
    for w_id in range(1, n_warehouses + 1):
        for d_id in range(1, DIST_PER_WARE + 1):
            customer(d_id, w_id)

def load_orders():
    for w_id in range(1, n_warehouses + 1):
        for d_id in range(1, DIST_PER_WARE + 1):
            orders(d_id, w_id)

def stock(w_id):
    s_w_id = w_id
    orig = [0 for _ in range(MAXITEMS)]
    for i in range(MAXITEMS // 10):
        pos = urand(0, MAXITEMS - 1)
        while orig[pos] == 1:
            pos = urand(0, MAXITEMS - 1)
        orig[pos] = 1
    
    for s_i_id in range(1, MAXITEMS + 1):
        s_quantity = urand(10, 100)
        s_dist_01 = alphastr(24, 24)
        s_dist_02 = alphastr(24, 24)
        s_dist_03 = alphastr(24, 24)
        s_dist_04 = alphastr(24, 24)
        s_dist_05 = alphastr(24, 24)
        s_dist_06 = alphastr(24, 24)
        s_dist_07 = alphastr(24, 24)
        s_dist_08 = alphastr(24, 24)
        s_dist_09 = alphastr(24, 24)
        s_dist_10 = alphastr(24, 24)
        s_data = alphastr(26, 50)
        sdatasiz = len(s_data)

        if orig[s_i_id - 1] == 1:
            s_data = list(s_data)
            pos = urand(0, sdatasiz - 8)
            s_data[pos+0] = 'o'
            s_data[pos+1] = 'r'
            s_data[pos+2] = 'i'
            s_data[pos+3] = 'g'
            s_data[pos+4] = 'i'
            s_data[pos+5] = 'n'
            s_data[pos+6] = 'a'
            s_data[pos+7] = 'l'
            s_data = ''.join(s_data)
        
        conn.execute('INSERT INTO STOCK (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data, s_ytd, s_order_cnt, s_remote_cnt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', [s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data, 0, 0, 0])

def district(w_id):
    d_w_id = w_id
    d_ytd = 30000
    d_next_o_id = 3001
    for d_id in range(1, DIST_PER_WARE + 1):
        d_name = alphastr(6, 10)
        d_street_1 = alphastr(10, 20)
        d_street_2 = alphastr(10, 20)
        d_city = alphastr(10, 20)
        d_state = alphastr(2, 2)
        d_zip = numstr(4, 4) + '11111'
        d_tax = urand(0, 2000) / 10000

        conn.execute('INSERT INTO DISTRICT (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', [d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id])

def customer(d_id, w_id):
    for c_id in range(1, CUST_PER_DIST + 1):
        c_d_id = d_id
        c_w_id = w_id
        c_first = alphastr(8, 16)
        c_middle = 'OE'
        if c_id <= 1000:
            c_last = lastname(c_id - 1)
        else:
            c_last = lastname(NURand_CLAST(0, 999))
        c_street_1 = alphastr(10, 20)
        c_street_2 = alphastr(10, 20)
        c_city = alphastr(10, 20)
        c_state = alphastr(2, 2)
        c_zip = numstr(4, 4) + '11111'
        c_phone = numstr(16, 16)
        if urand(1, 2) < 2:
            c_credit = 'GC'
        else:
            c_credit = 'BC'
        c_credit_lim = 50000
        c_discount = urand(0, 50) / 100
        c_balance = -10
        c_data = alphastr(300, 500)

        conn.execute('INSERT INTO CUSTOMER (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment, c_payment_cnt, c_delivery_cnt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', [c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, timestamp, c_credit, c_credit_lim, c_discount, c_balance, c_data, 10, 1, 0])

        h_amount = 10
        h_data = alphastr(12, 24)

        conn.execute('INSERT INTO HISTORY (h_c_id, h_c_d_id, h_c_w_id, h_w_id, h_d_id, h_date, h_amount, h_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', [c_id, c_d_id, c_w_id, c_w_id, c_d_id, timestamp, h_amount, h_data])

def orders(d_id, w_id):
    cust_ids = permutation(range(1, CUST_PER_DIST + 1))
    for o_id in range(1, ORD_PER_DIST + 1):
        o_c_id = cust_ids[o_id - 1]
        o_carrier_id = urand(1, 10)
        o_ol_cnt = urand(5, 15)

        if o_id > 2100:
            conn.execute('INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', [o_id, o_c_id, d_id, w_id, timestamp, None, o_ol_cnt, 1])
            conn.execute('INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (%s, %s, %s)', [o_id, d_id, w_id])
        else:
            conn.execute('INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', [o_id, o_c_id, d_id, w_id, timestamp, o_carrier_id, o_ol_cnt, 1])
        
        for ol in range(1, o_ol_cnt + 1):
            ol_i_id = urand(1, MAXITEMS)
            ol_supply_w_id = w_id
            ol_quantity = 5
            ol_amount = 0

            ol_dist_info = alphastr(24, 24)

            if o_id > 2100:
                conn.execute('INSERT INTO ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_delivery_d) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', [o_id, d_id, w_id, ol, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, None])
            else:
                conn.execute('INSERT INTO ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_delivery_d) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', [o_id, d_id, w_id, ol, ol_i_id, ol_supply_w_id, ol_quantity, urand(10, 10000) / 100, ol_dist_info, timestamp])

def create_tables():
    print('+ creating tables')
    conn.execute(open('table_def.sql', 'r').read())
    print('+ creating foreign keys')
    conn.execute(open('fk_def.sql', 'r').read())

if __name__ == '__main__':
    args = create_arguments()
    n_warehouses = args.warehouses

    conn = psycopg.connect(' '.join(args.db_connect_str))

    print(f'+++ loading TPC-C database (warehouses: {n_warehouses})')

    if args.create_tables:
        create_tables()

    print('+ loading items')
    load_items()
    print('+ loading warehouses')
    load_warehouse()
    print('+ loading customers')
    load_cust()
    print('+ loading orders')
    load_orders()
    print('+ committing...')
    conn.commit()
    print('! complete')

    conn.close()
