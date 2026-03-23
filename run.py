import argparse
import psycopg
import datetime
import decimal
import time
from os import path
from rand import NURand_CID, NURand_OLIID, NURand_CLAST, urand, lastname

MAXITEMS = 100000
DIST_PER_WARE = 10
n_warehouses = 0

class DatabaseConnection:
    def __init__(self, dbstring, outpath = None):
        self._conn = psycopg.connect(dbstring)
        self._cur = self._conn.cursor()
        self._client_cursor = psycopg.ClientCursor(self._conn)
        self._outpath = outpath
        self.closed = False
        self._trans = None
        self._log_is_disabled = False
        self._statements = {}
    
    def close(self):
        self._ensure_open()
        if self._trans is not None:
            print('!! WARN: closing database connection without committing transaction', self._trans)
            self._conn.rollback()
        self._cur.close()
        self._conn.close()
        self.closed = True
    
    def _ensure_open(self):
        if self.closed:
            raise ConnectionError('attempting database operation after connection was closed!')
    
    def transact(self, name):
        self._ensure_open()
        if self._trans is not None:
            print('!! WARN: starting new transaction', name, 'when', self._trans, 'was not committed!')
            self._conn.rollback()
        
        self._trans = name
    
    def execute(self, statement, params = None, no_log = False):
        self._ensure_open()
        if self._trans is None:
            self.transact('default')
        
        self._cur.execute(statement, params)

        if not (no_log or self._log_is_disabled):
            text = list(self._client_cursor.mogrify(statement, params))
            if text[-1] != ';':
                text.append(';')
            text = ''.join(text)
            if statement not in self._statements:
                self._statements[statement] = []
            self._statements[statement].append(text)
    
    def _write(self):
        if self._outpath is None: return
        for i_s, entry in enumerate(self._statements.items()):
            statement = entry[0]
            queries = entry[1]
            for i_q, query in enumerate(queries):
                with open(path.join(self._outpath, f'{self._trans}_{i_s}_{i_q}.sql'), 'w') as outfile:
                    outfile.write(query)
        
        self._statements = {}
    
    def log_disable(self):
        self._log_is_disabled = True
    
    def log_enable(self):
        self._log_is_disabled = False
    
    def commit(self):
        self._ensure_open()
        self._conn.commit()

        self._write()

        self._trans = None
    
    def rollback(self):
        self._ensure_open()
        self._conn.rollback()
        
        self._write()

        self._trans = None
    
    def fetchone(self):
        self._ensure_open()
        return self._cur.fetchone()
    
    def fetchmany(self, size: int = 0):
        self._ensure_open()
        return self._cur.fetchmany(size)

    def fetchall(self):
        self._ensure_open()
        return self._cur.fetchall()

sql: DatabaseConnection = None

def create_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('n_warehouses', type=int)
    parser.add_argument('db_connect_str', nargs='+', type=str)

    return parser.parse_args()

def new_order(w_id):
    timestamp = datetime.datetime.now()
    sql.transact('NEW_ORDER_TRANS')

    d_id = urand(1, 10)
    c_id = NURand_CID(1, 3000 + 1)
    o_ol_cnt = urand(5, 15)

    sql.execute('SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = %s AND c_w_id = w_id AND c_d_id = %s AND c_id = %s', [w_id, d_id, c_id])
    result = sql.fetchone()
    c_discount = result[0]
    c_last = result[1]
    c_credit = result[2]
    w_tax = result[3]

    sql.execute('SELECT d_next_o_id, d_tax FROM district WHERE d_id = %s AND d_w_id = %s', [d_id, w_id])
    result = sql.fetchone()
    d_next_o_id = result[0]
    d_tax = result[1]

    sql.execute('UPDATE district SET d_next_o_id = %s WHERE d_id = %s AND d_w_id = %s', [d_next_o_id + 1, d_id, w_id])
    o_id = d_next_o_id
    ol_all_local = 1

    sql.execute('INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (%s, %s, %s, %s, %s, %s, %s)', [o_id, d_id, w_id, c_id, timestamp, o_ol_cnt, ol_all_local])
    sql.execute('INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (%s, %s, %s)', [o_id, d_id, w_id])

    price = []
    name = []

    for ol_number in range(1, o_ol_cnt + 1):
        ol_quantity = urand(1, 10)
        ol_supply_w_id = w_id if urand(1, 100) > 1 else urand(1, n_warehouses)
        i_id = urand(1, MAXITEMS)

        sql.execute('SELECT i_price, i_name, i_data FROM item WHERE i_id = %s', [i_id])
        result = sql.fetchone()
        if result is None:
            sql.rollback()
            return
        i_price = result[0]
        i_name = result[1]
        i_data = result[2]

        sql.execute('SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = %s AND s_w_id = %s', [i_id, ol_supply_w_id])
        result = sql.fetchone()
        s_quantity = result[0]
        s_data = result[1]
        ol_dist_info = result[d_id + 1]
        original = ('original' in i_data) and ('original' in s_data)
        bg = 'b' if original else 'g'

        if s_quantity > ol_quantity:
            s_quantity -= ol_quantity
        else:
            s_quantity = s_quantity - ol_quantity + 91
        
        sql.execute('UPDATE stock SET s_quantity = %s WHERE s_i_id = %s AND s_w_id = %s', [s_quantity, i_id, ol_supply_w_id])

        ol_amount = ol_quantity * i_price * (1+w_tax+d_tax) * (1-c_discount)

        sql.execute('INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', [o_id, d_id, w_id, ol_number, i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info])
    
    sql.commit()

def payment(w_id):
    h_amount = round(decimal.Decimal(urand(100, 500000) / 100), 2)
    timestamp = datetime.datetime.now()
    d_id = urand(1, 10)
    by_name = urand(1, 100) <= 60
    c_w_id = w_id if urand(1, 100) <= 85 else urand(1, n_warehouses)
    c_d_id = d_id if c_w_id == w_id else urand(1, DIST_PER_WARE)

    sql.transact('PAYMENT_TRANS')
    sql.execute('UPDATE warehouse SET w_ytd = w_ytd + %s WHERE w_id = %s', [h_amount, w_id])
    sql.execute('SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = %s', [w_id])
    warehouse_loc = sql.fetchone()
    w_name = warehouse_loc[5]
    sql.execute('UPDATE district SET d_ytd = d_ytd + %s WHERE d_w_id = %s AND d_id = %s', [h_amount, w_id, d_id])
    sql.execute('SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = %s AND d_id = %s', [w_id, d_id])
    district_loc = sql.fetchone()
    d_name = district_loc[5]

    if by_name:
        c_last = NURand_CLAST(0, 999)
        c_last = lastname(c_last)

        sql.execute('SELECT count(c_id) FROM customer WHERE c_last = %s AND c_d_id = %s AND c_w_id = %s', [c_last, c_d_id, c_w_id])
        namecnt = sql.fetchone()[0]
        sql.execute('SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = %s AND c_d_id = %s AND c_last = %s ORDER BY c_first', [c_w_id, c_d_id, c_last])

        if namecnt % 2 == 1:
            namecnt += 1

        for n in range(namecnt // 2):
            cust_info = sql.fetchone()
            c_first = cust_info[0]
            c_middle = cust_info[1]
            c_id = cust_info[2]
            c_street_1 = cust_info[3]
            c_street_2 = cust_info[4]
            c_city = cust_info[5]
            c_state = cust_info[6]
            c_zip = cust_info[7]
            c_phone = cust_info[8]
            c_credit = cust_info[9]
            c_credit_lim = cust_info[10]
            c_discount = cust_info[11]
            c_balance = cust_info[12]
            c_since = cust_info[13]
    else:
        c_id = NURand_CID(1, 3000)
        sql.execute('SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s', [c_w_id, c_d_id, c_id])
        cust_info = sql.fetchone()
        c_first = cust_info[0]
        c_middle = cust_info[1]
        c_last = cust_info[2]
        c_street_1 = cust_info[3]
        c_street_2 = cust_info[4]
        c_city = cust_info[5]
        c_state = cust_info[6]
        c_zip = cust_info[7]
        c_phone = cust_info[8]
        c_credit = cust_info[9]
        c_credit_lim = cust_info[10]
        c_discount = cust_info[11]
        c_balance = cust_info[12]
        c_since = cust_info[13]
    
    c_balance += h_amount
    h_data = w_name + '    ' + d_name
    if c_credit == 'BC':
        sql.execute('SELECT c_data FROM customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s', [c_w_id, c_d_id, c_id])
        c_data = sql.fetchone()[0]
        c_new_data = '| %4d %2d %4d %2d %4d $%7.2f %12s %24s' % (c_id, c_d_id, c_w_id, d_id, w_id, h_amount, timestamp, h_data)
        c_data = [*list(c_new_data), *list(c_data)]
        c_data = c_data[:500]
        c_data = ''.join(c_data)
        sql.execute('UPDATE customer SET c_balance = %s, c_data = %s WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s', [c_balance, c_data, c_w_id, c_d_id, c_id])
    else:
        sql.execute('UPDATE customer SET c_balance = %s WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s', [c_balance, c_w_id, c_d_id, c_id])
    
    sql.execute('INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', [c_d_id, c_w_id, c_id, d_id, w_id, timestamp, h_amount, h_data])
    sql.commit()

def order_status(w_id):
    d_id = urand(1, 10)
    by_name = urand(1, 100) <= 60

    sql.transact('ORDER_STATUS_TRANS')

    if by_name:
        c_last = NURand_CLAST(0, 999)
        c_last = lastname(c_last)

        sql.execute('SELECT count(c_id) FROM customer WHERE c_last = %s AND c_d_id = %s AND c_w_id = %s', [c_last, d_id, w_id])
        namecnt = sql.fetchone()[0]
        sql.execute('SELECT c_balance, c_first, c_middle, c_id FROM customer WHERE c_w_id = %s AND c_d_id = %s AND c_last = %s ORDER BY c_first', [w_id, d_id, c_last])

        if namecnt % 2 == 1:
            namecnt += 1

        for n in range(namecnt // 2):
            cust_info = sql.fetchone()
            c_balance = cust_info[0]
            c_first = cust_info[1]
            c_middle = cust_info[2]
            c_id = cust_info[3]
    else:
        c_id = NURand_CID(1, 3000)
        sql.execute('SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s', [c_id, d_id, w_id])
        cust_info = sql.fetchone()
        c_balance = cust_info[0]
        c_first = cust_info[1]
        c_middle = cust_info[2]
        c_last = cust_info[3]
    
    sql.execute('SELECT o_id, o_carrier_id, o_entry_d FROM orders ORDER BY o_id DESC')
    order = sql.fetchone()
    o_id = order[0]

    sql.execute('SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s', [o_id, d_id, w_id])

    # simulate iterating over all rows
    while True:
        order_line = sql.fetchone()
        if order_line is None:
            break
    
    sql.commit()

def delivery(w_id):
    timestamp = datetime.datetime.now()
    o_carrier_id = urand(1, 10)

    sql.transact('DELIVERY_TRANS')

    for d_id in range(1, DIST_PER_WARE + 1):
        sql.execute('SELECT no_o_id FROM new_order WHERE no_d_id = %s AND no_w_id = %s ORDER BY no_o_id ASC', [d_id, w_id])
        no_o_id = sql.fetchone()

        if no_o_id is None:
            continue

        no_o_id = no_o_id[0]

        sql.execute('DELETE FROM new_order WHERE no_o_id = %s AND no_d_id = %s AND no_w_id = %s', [no_o_id, d_id, w_id])
        sql.execute('SELECT o_c_id FROM orders WHERE o_id = %s AND o_d_id = %s AND o_w_id = %s', [no_o_id, d_id, w_id])
        c_id = sql.fetchone()[0]
        sql.execute('UPDATE orders SET o_carrier_id = %s WHERE o_id = %s AND o_d_id = %s AND o_w_id = %s', [o_carrier_id, no_o_id, d_id, w_id])
        sql.execute('UPDATE order_line SET ol_delivery_d = %s WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s', [timestamp, no_o_id, d_id, w_id])
        sql.execute('SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s', [no_o_id, d_id, w_id])
        ol_total = sql.fetchone()[0]
        sql.execute('UPDATE customer SET c_balance = c_balance + %s WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s', [ol_total, c_id, d_id, w_id])
    
    sql.commit()

def stock_level(w_id, d_id):
    threshold = urand(10, 20)

    sql.transact('STOCK_LEVEL_TRANS')

    sql.execute('SELECT d_next_o_id FROM district WHERE d_w_id = %s AND d_id = %s', [w_id, d_id])
    o_id = sql.fetchone()[0]

    sql.execute('SELECT COUNT(DISTINCT(s_i_id)) FROM order_line, stock WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id < %s AND ol_o_id >= %s - 20 AND s_w_id = %s AND s_i_id = ol_i_id AND s_quantity < %s', [w_id, d_id, o_id, o_id, w_id, threshold])

    sql.commit()

if __name__ == '__main__':
    args = create_arguments()
    n_warehouses = args.n_warehouses
    sql = DatabaseConnection(' '.join(args.db_connect_str), 'workload/')

    HOME_W_ID = urand(1, n_warehouses)
    HOME_D_ID = urand(1, DIST_PER_WARE)

    print(f'+ starting TPC-C benchmark (home warehouse: {HOME_W_ID}, district: {HOME_D_ID})')
    tic = time.time()
    print('- new-order')
    new_order(HOME_W_ID)
    print('- payment')
    payment(HOME_W_ID)
    print('- order-status')
    order_status(HOME_W_ID)
    print('- delivery')
    delivery(HOME_W_ID)
    print('- stock-level')
    stock_level(HOME_W_ID, HOME_D_ID)
    toc = time.time()
    print(f'+ complete in {round(toc - tic, 4)}s')

    sql.close()
