import psycopg2 as pg


class DBManagement:
    def __init__(self, host, dbname, user, password):
        self.conn = pg.connect("host = {} dbname={} user={} password={} port=5432".format(host, dbname, user, password))
        self.cur = self.conn.cursor()

    def get_gene_symbol(self, geneid):
        sql = "SELECT symbol FROM gene WHERE geneid = '{}' and ncbitaxid = '9606'".format(geneid)
        self.cur.execute(sql)
        symbol = self.cur.fetchone()[0]
        return symbol

    def close_connection(self):
        self.conn.close()
