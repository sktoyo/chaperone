import DBMangement
import pandas as pd


def get_gp_list_from_left(dbm, bp_name):
    """
        Get geneproduct id list from biological process name. The biological process name locate on the right side of
        knowledge units.
        :param dbm:
        :param bp_name: biological process name
        :return: gene product id list
    """
    query = "select ku.leftentityid from knowledgeunit ku JOIN biologicalprocess bp ON ku.rightentityid = bp.biologicalprocessid where bp.name like '%{}%' and ku.lefttype = 'GP'".format(bp_name)
    dbm.cur.execute(query)
    left_gp_result = dbm.cur.fetchall()
    left_gp_result = [gpid[0] for gpid in left_gp_result]
    left_gp_result = list(set(left_gp_result))
    return left_gp_result


def get_gp_list_from_right(dbm, bp_name):
    """
        Get geneproduct id list from biological process name. The biological process name locate on the left side of
        knowledge units.
        :param dbm:
        :param bp_name: biological process name
        :return: gene product id list
        """
    query = "select ku.leftentityid from knowledgeunit ku JOIN biologicalprocess bp ON ku.leftentityid = bp.biologicalprocessid where bp.name like '%{}%' and ku.righttype = 'GP'".format(bp_name)
    dbm.cur.execute(query)
    right_gp_result = dbm.cur.fetchall()
    right_gp_result = [gpid[0] for gpid in right_gp_result]
    right_gp_result = list(set(right_gp_result))
    return right_gp_result


def get_symbol_list(dbm, gpid_list):
    """
        Get symbol list from gene product id list
        :param dbm:
        :param gpid_list: gene product id list
        :return: gene symbol list
    """
    symbol_list = list()
    for gpid in gpid_list:
        sql = "SELECT symbol FROM geneproduct WHERE geneproductid = '{}' and speciesid = 'SP00007755'".format(gpid)
        dbm.cur.execute(sql)
        symbol = dbm.cur.fetchone()[0]
        symbol_list.append(symbol)
    return symbol_list


def write_symbol_tsv(symbol_list, bp_name):
    """
        Write symbol list into tsv format file
        :param symbol_list:
        :param bp_name:
        :return: None
    """
    symbol_list = list(set(symbol_list))
    symbol_list.sort()
    pd.DataFrame({"symbol": symbol_list}).to_csv("../data/gene/{}_symbol.tsv".format(bp_name), sep='\t', index=False)


def main(bp_name, dbm):
    """
        Find and write symbol of genes that related to the biological process
        :param bp_name:
        :param dbm:
        :return:
    """
    gpid_list = get_gp_list_from_left(dbm, bp_name)
    symbol_list = get_symbol_list(dbm, gpid_list)
    write_symbol_tsv(symbol_list, bp_name)


if __name__ == "__main__":
    host = ""
    dbname = ""
    user = ""
    password = ""
    dbm = DBMangement.DBManagement(host, dbname, user, password)
    bp_name = "chaperone"
    main("chaperone", dbm)
    main("proteasome", dbm)
    dbm.cur.close()