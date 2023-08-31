import re,base64,random,string
import time
# from app.decorators.Decorator import row_dict
# from app.utils import select_loan_bind
# from app import db

from app.business.common import configcenter
import requests
import json


class CheckFormat():
    def __init__(self):
        pass
    #check sqlserver
    def check_select_sql_format(self,entity, *DBServerInfo, is_check_table=True,is_check_column=True):
        split_result = []
        # 将传入的一个或者多个sql以逗号拆分保存成list
        sql_list = split_sql_list(entity["SqlStr"])
        # 分别检查每一个sql的格式是否满足需求，并将拆分结果保存并返回
        for i in sql_list:
            if i.strip() != '':
                # if is_check_column==False:
                #     i=i.upper().split("WHERE ",1)[0]
                check_select_sql = check_one_select_sql_format(i, entity["ConnectionStr"],is_check_column)
                # 判断是否有异常返回
                if isinstance(check_select_sql, tuple):
                    return check_select_sql
                # if check_select_sql.__contains__("IsSuccess"):
                #     return {"IsSuccess": False,
                #             "Reason": "SQL format is not correct, please check your sql.",
                #             "SqlStr": i,
                #             "Count": 0,
                #             "Data": None}, 500
                # 检查返回结果是否有table
                elif check_select_sql.__contains__("TableList"):
                    if len(check_select_sql["TableList"]) == 0:
                        return {"IsSuccess": False,
                                "Reason": "SQL format is not correct, please check your sql.",
                                "SqlStr": i,
                                "Count": 0,
                                "Data": None}
                    else:
                        split_result.append(check_select_sql)
                else:
                    return check_select_sql
        # 判断返回的表和字段是否真实存在
        if split_result is not None and is_check_table == True:
            # print("start :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            try:
                if len(DBServerInfo)>0:
                    Server = DBServerInfo[0]
                    Port = DBServerInfo[1]
                    User = DBServerInfo[2]
                    Password = DBServerInfo[3]
                else:
                    Server = None
                    Port = None
                    User = None
                    Password = None
                if entity.get("ExecuteEnv",None)==None:
                    ExecuteEnv="GDEV"
                else:
                    ExecuteEnv=entity.get("ExecuteEnv",None)
                check_column = check_table(split_result, entity["ConnectionStr"], ExecuteEnv,is_check_column,Server, Port, User, Password)
            except Exception as f:
                raise Exception(f.args[0])
            # print("end :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            if check_column == True:
                # return {"IsSuccess": True, "Data": split_result, "Count": len(split_result), "Reason": None,
                #         "SqlStr": None}
                return split_result
            else:
                return check_column
        elif split_result is not None and is_check_table == False:
            # return {"IsSuccess": True, "Data": split_result, "Count": len(split_result), "Reason": None,
            #         "SqlStr": None}
            return split_result
    #check_mysql
    def check_select_sql_format_mysql(self,entity, is_check_table=True,is_check_column=True):
        # 将传入的一个或者多个sql以逗号拆分保存成list
        sql_list = split_sql_list(entity["SqlStr"])
        # 分别检查每一个sql的格式是否满足需求，并将拆分结果保存并返回
        try:
            for i in sql_list:
                if i.strip() != '':
                    if 'LIMIT' in i.upper().split(' ') or 'JOIN' in i.upper().split(' '):
                        return {"IsSuccess": False,
                                    "Reason": "SQL format is not correct, Does not support LIMIT/JOIN",
                                    "SqlStr": i,
                                    "Count": 0,
                                    "Data": None}
                    dbname = check_select_sql_format_mysql(i)
                    # 检查是否有dbname
                    if dbname is not None:
                        count = dbname.count('.',1,-1)
                        if count == 1:
                           gettable_PRIMARY(entity['ConnectionStr'], entity['ExecuteEnv'], dbname)
                        else:
                            return {"IsSuccess": False,
                                    "Reason": "SQL format is not correct, DataBase must be included",
                                    "SqlStr": i,
                                    "Count": 0,
                                    "Data": None}
                    else:
                        return {"IsSuccess": False,
                                "Reason": "SQL format is not correct, TableName must be included.",
                                "SqlStr": i,
                                "Count": 0,
                                "Data": None}
                    # 检查返回结果是否有tabl
            # 判断返回的表和字段是否真实存在
            # split_result = []
            # sql_list = split_sql_list(entity["SqlStr"])
            # for sql in sql_list:
            #     if sql.strip() != '':
            return []
        except BaseException as e:
            return {"IsSuccess": False,
                    "Reason": "SQL format is not correct, please check your sql.",
                    "Reason": str(e),
                    "Data": None}
def check_select_sql_format_mysql(sql:str) -> str:
    try:
        SqlStr = sql.upper()
        sqlstrlist = SqlStr.split(' ')
        fromindex = sqlstrlist.index('FROM')
    except Exception:
        return None
    if fromindex == 2:
        if sqlstrlist[0] != 'SELECT' or sqlstrlist[1] != "*":
            raise Exception('SQL format is not correct, please check your sql.')
        else:
            tablename = sqlstrlist[fromindex+1]
            tablename = tablename.replace('`','').replace('\'','')
            return tablename.lower()
    else:
        raise Exception('SQL format is not correct, please check your sql.')


def gettable_PRIMARY(ConnectionStr, dbenv, tablename):
    SqlStr = f"SHOW INDEX IN {tablename}"
    result = dbrequest(SqlStr, dbenv, ConnectionStr).json()
    if result.get('IsSuccess'):
        keylist =  [i.get('Column_name') for i in result.get('data') if i.get('Key_name')=='PRIMARY']
        return keylist[0]
    else:
        raise BaseException('SQL Exec Fail')


def dbrequest(sql, dbenv, ConnectionStr, DBServerInfo=None, isSelect=True):
    if DBServerInfo:
        if DBServerInfo[0] != None:
            Server = DBServerInfo[0]
            Port = DBServerInfo[1]
            User = DBServerInfo[2]
            Password = DBServerInfo[3]
            return call_exec_sql_v2(dbenv, Server, Port, User, Password, sql, isSelect)
    return call_exec_sql_v1(dbenv, ConnectionStr, sql)

# 检查sql中的表名是否存在，是否包含a.b.c的3段结构，并调用check_table_exists（）检查表是否存在，是否是Table
def check_table(lists, conn, env,is_check_column, *DBServerInfo):
    # i=0
    # m=0
    checked_list=[]
    for list in lists:
        for di in list["TableList"]:
            # if di["Type"]=="Table":
            if di["Name"] in checked_list:
                continue;
            split_table_name = di["Name"].split(".")
            if len(split_table_name) != 3:
                return {"IsSuccess": False,
                        "Reason": "Table name need contain database name, please check your sql.",
                        "SqlStr": di["Name"],
                        "Count": 0,
                        "Data": None
                        }
            elif len(split_table_name) == 3:
                # 检查table是否存在
                # table_column_list=check_table_exists(split_table_name,conn)
                try:
                    Server = DBServerInfo[0]
                    Port = DBServerInfo[1]
                    User = DBServerInfo[2]
                    Password = DBServerInfo[3]
                    table_column_list = check_table_exists(di, split_table_name, env, Server, Port, User, Password,is_check_column)
                except Exception as f:
                    # print(table_column_list
                    # if table_column_list is None:
                    raise Exception(f.args[0])
                    # return {"IsSuccess": False,
                    #         "Reason": "Table [%s] is not exists in current db, please check your sql." % di["Name"],
                    #         "Error": str(f)
                    #         }, 500
                if type(table_column_list) is dict and table_column_list["IsSuccess"] == False:
                    return table_column_list
                if is_check_column==True:
                    check_column = check_column_exsits(di, list, table_column_list)
                    if check_column != True:
                        raise Exception(check_column["Reason"])
                    checked_list.append(di["Name"])
                    table_name = split_table_name[0] + "." + split_table_name[1] + "." + split_table_name[2]
                    if len(table_column_list) == 0:
                        return {"IsSuccess": False,
                                "Reason": "Table [%s] is not exsits in current db, please check your sql." % table_name,
                                "SqlStr": None,
                                "Count": 0,
                                "Data": None
                                }
                    elif table_column_list["data"][0]["TableType"].upper().strip() != 'U':
                        return {"IsSuccess": False,
                                "Reason": "Table [%s] is not a Table,Only support table backup" % table_name,
                                "SqlStr": None,
                                "Count": 0,
                                "Data": None
                                }
                # elif check_column != True:
                #     return check_column

    return True


# 检查sql中的column是否包含在对应的表中
def check_column_exsits(di, list, checklist):
    # 将所有待检查的字段保存到column_list中
    column_list = []
    if di["Alias"] != "":
        for i in list["ConditionList"]:
            if i["Alias"] == di["Alias"]:
                column_list.append(i["Name"].replace("[","").replace("]",""))
    else:
        for i in list["ConditionList"]:
            # if i["Type"]=="Condition":
            column_list.append(i["Name"].replace("[","").replace("]",""))
    # 检查所涉及的查询条件是否在表中存在，如果不存在返回失败，如果成功返回True
    if column_list != None:
        for column in column_list:
            count = 0
            for check_dic in checklist["data"]:
                if check_dic["ColumnName"].upper() == column.upper():
                    count += 1
            if count == 0:
                return {"IsSuccess": False,
                        "Reason": "ColumnName [%s] is not exsits in current server, please check your sql." % column,
                        "Count": 0,
                        "SqlStr": None,
                        "Data": None
                        }
    return True


# 检查表是否存在
# @row_dict
def check_table_exists(di, table_name_list, conn_env, *DBServerInfo):
    bind = di["ConnectionStr"]
    # print(table_name_list[0])
    table_name = table_name_list[0] + "." + table_name_list[1] + "." + table_name_list[2]
    # parameter={"TableName":table_name_list[1]+"."+table_name_list[2]}
    SqlParameter = {"TableName": di["Name"]}
    # 手动替换db name
    # get_table_column_priority_db = get_table_column_priority % table_name_list[0]
    get_table_column_priority_db = configcenter.sqlconfig.get("get_table_column_priority") % (
    table_name_list[0], table_name)
    try:
        # re = db.session.execute(get_table_column_priority_db, parameter, bind=bind)
        # if conn_env.upper()=="LDEV":
        #     conn="LDEV_"+bind
        # url="http://gdev-services.newegg.space/29f31032/api/control_execsql"

        if DBServerInfo[0] is not None:
            Server = DBServerInfo[0]
            Port= DBServerInfo[1]
            User= DBServerInfo[2]
            Password= DBServerInfo[3]
            re = call_exec_sql_v2(conn_env, get_table_column_priority_db, Server, Port, User, Password)
        else:
            re = call_exec_sql_v1(conn_env, bind, get_table_column_priority_db)
        c = re.json()

        if len(c.get("data",[])) == 0:
            raise Exception( "Table [%s] is Not Exists." % table_name)
        else:
            if c["data"][0]['TableType'].strip().upper()=='V':
                raise Exception( "View [%s] is Not support ." % table_name)
        return re.json()
    except Exception as e:
        raise Exception(e.args[0])
    # return m

def call_exec_sql_v1(conn_env, bind, get_table_column_priority_db):
    url = configcenter.urlconfig.get("exec_sql_url")
    body = {"ExecuteEnv": conn_env,
            "ConnectionStr": bind,
            "SqlStr": get_table_column_priority_db
            # "SqlParameter": SqlParameter
            }
    key = str(int(round(time.time() * 1000))) + "_supr"
    key_64 = str(base64.b64encode(key.encode("ascii")), "utf-8") + ''.join(
        random.sample(string.ascii_letters + string.digits, 2))
    header = {'Content-Type': 'Application/json', 'Accept': 'Application/json', "Rolekey": key_64}
    re = requests.post(url=url, data=json.dumps(body), headers=header)
    # c = re.json()
    return re

def call_exec_sql_v2(conn_env, get_table_column_priority_db, Server, Port, User, Password):
    url = configcenter.urlconfig.get("exec_sql_url_v2")
    body = {"ExecuteEnv": conn_env,
             "Server": Server,
    "Port": Port,
    "User": User,
    "Password": Password,
            "SqlStr": get_table_column_priority_db
            }
    header = {'Content-Type': 'Application/json', 'Accept': 'Application/json'}
    print(url)
    print(json.dumps(body))

    re = requests.post(url=url, data=json.dumps(body), headers=header)
    # c = re.json()
    return re


# 检查传入的表名是否包含a.b.c的3段结构和是否包含 * 在select语句中
def check_one_select_sql_format(s, conn,is_check_column):
    # 去掉sql中多余的空格
    str = ' '.join(s.split())
    # 检查sql中包含的语句是否包含不符合的语句，如UPDATE,DELETE,INSERT,LEFT JOIN,RIGHT JOIN
    check_format = check_sql_format(str)
    if check_format != True:
        return check_format
    # 检查sql中是否包含*或者x.*的字符
    if check_is_include_star(str):
        # 初步的拆分sql为几大类型，并返回list
        initial_split_list = primary_split_str(str)
        if initial_split_list == None:
            return {"IsSuccess": False,
                    "Reason": "This sql format is not right,please check your sql.",
                    "SqlStr": str,
                    "Count": 0,
                    "Data": None}
        else:
            # 进一步的用正则匹配sql的格式，将传入的表与字段分离出来，并返回分离后的结果
            return forward_split_str(initial_split_list, str, conn,is_check_column)
    else:
        return {"IsSuccess": False,
                "Reason": "Select sql must use * to backup all column.",
                "SqlStr": str,
                "Count": 0,
                "Data": None
                }


def check_sql_format(s):
    str = s.upper()
    t = "UPDATE "
    t_re = [t.start() for t in re.finditer(t, str)]
    if len(t_re) == 0:
        t_re.append(1)
    # s = "SELECT "
    m = "INSERT "
    m_re = [m.start() for m in re.finditer(m, str)]
    if len(m_re) == 0:
        m_re.append(1)
    l = "DELETE "
    l_re = [l.start() for l in re.finditer(l, str)]
    if len(l_re) == 0:
        l_re.append(1)
    k = "LEFT JOIN "
    p = "RIGHT JOIN "
    # result_one = t_re[0]+ m_re[0] + l_re[0]
    result = str.count(k) + str.count(p)
    if t_re[0] == 0 or m_re[0] == 0 or l_re[0] == 0 or result > 0 or ' UNION ' in str:
        return {"IsSuccess": False,
                "Reason": "Backup SQL format is not correct,please check it! Only support format: select...|select ...where...| select ...inner join ... where... ",
                "SqlStr": str,
                "Count": 0,
                "Data": None

                }
    else:
        return True


# 将sql用分号拆分，并且返回list
def split_sql_list(str):
    return str.split(";")


# 匹配select 语句是否包含*的部分
def check_is_include_star(sql):
    regex = r"(?:(?:[a-zA-Z0-9_]?\.\*)|\*)"
    mat = re.findall(regex, sql)
    if len(mat) > 0:
        return True
    else:
        return False


# 保存拆分结果，排除掉重复的部分
def check_duplicate_column(d, check_list):
    count = 0
    for i in check_list:
        if d != i:
            count += 1
    if count == len(check_list):
        return True
    else:
        return False


# 初步拆分sql，拆分inner join 、on、where、or、between、and
def primary_split_str(str):
    str_replace= str.upper().replace("\n", " ").replace("WITH(NOLOCK)", "").replace("WITH ( NOLOCK )", "").replace("WITH ( NOLOCK)", "").replace("WITH (NOLOCK )", "").replace("WITH (NOLOCK)", "").replace(
        "(NOLOCK)", "").replace(" AS ",
                              " ")
    # print(str)
    if(str_replace.__contains__("IN(")):
        str_replace_n = str_replace.split("IN(")[0]+"IN ()"
    elif(str_replace.__contains__("IN (")):
        str_replace_n = str_replace.split("IN (")[0]+"IN ()"
    else:
        str_replace_n=str_replace
    s_split_innerjoin = str_replace_n.split("INNER JOIN")
    li_split_on = []
    li_split_where = []
    list_split_and = []
    list_split_or = []
    # list_between = []
    list_split_result = []
    for m in s_split_innerjoin:
        li_split_on.extend(m.split(" ON "))
    for n in li_split_on:
        li_split_where.extend(n.split(" WHERE "))
    for t in li_split_where:
        list_split_and.extend(t.split(" AND "))
    for a in list_split_and:
        list_split_or.extend(a.split(" OR "))
    for b in list_split_or:
        list_split_result.extend(b.split(" BETWEEN "))
    # for b in list_split_or:
    #     list_split_result.extend(b.split(" IN "))
    # for b in list_split_or:
    #     list_split_result.extend(b.split("!="))
    # for b in list_split_or:
    #     list_split_result.extend(b.split("<>"))
    # for b in list_split_or:
    #     list_split_result.extend(b.split(">"))
    # for b in list_split_or:
    #     list_split_result.extend(b.split("<"))
    # print(list_split_result)
    return list_split_result


# 进一步使用正则表达式拆分表名和字段名以及别名的部分
def forward_split_str(s, str, conn,is_check_column):
    table_result = []
    conditon_result = []
    result_dict = {}
    # regex_first_table = r"(?:(?:UPDATE)|(?:FROM))[\s\n]+((?:(?:[a-zA-Z0-9_]+?)|(?:\[[a-zA-Z0-9_]+?\]))\.(?:(?:[a-zA-Z0-9_]*?)|(?:\[[a-zA-Z0-9_]*?\]))\.(?:(?:[a-zA-Z0-9_]+)|(?:\[[a-zA-Z0-9_]+\])))+([\s\n]?(?:[a-zA-Z0-9_]+)?)"
    regex_first_table = r"(?:(?:UPDATE)|(?:FROM))[\s\n]+((?:[a-zA-Z0-9_]+\.)?(?:(?:[a-zA-Z0-9_]+?)|(?:\[[a-zA-Z0-9_]+?\]))\.(?:(?:[a-zA-Z0-9_]*?)|(?:\[[a-zA-Z0-9_]*?\]))\.(?:(?:[a-zA-Z0-9_]+)|(?:\[[a-zA-Z0-9_]+\])))+([\s\n]*(?:[a-zA-Z0-9_]+)?)"
    regex_inner_join_table = r"((?:[a-zA-Z0-9_]+\.)?(?:(?:[a-zA-Z0-9_]+?)|(?:\[[a-zA-Z0-9_]+?\]))\.(?:(?:[a-zA-Z0-9_]*?)|(?:\[[a-zA-Z0-9_]*?\]))\.(?:(?:[a-zA-Z0-9_]+)|(?:\[[a-zA-Z0-9_]+\])))+([\s\n]?(?:[a-zA-Z0-9_]+)?)"
    regex_condition = r"([^=]*)[?==]([\s\n]?[^=]*)"
    # regex_condition_in = r"([^=]*)[\s\n]?IN([\s\n]?[^=]*)"
    regex_condition_in ="([^=]*)[\s\n]?IN[\s\n]?(\([^=]*\))"
    regex_condition_not_equals_a = r"([^=]*)!=([\s\n]?[^=]*)"
    regex_condition_not_equals_b = r"([^=]*)<>([\s\n]?[^=]*)"
    regex_condition_not_equals_greater_than = r"([^=]*)>([\s\n]?[^=]*)"
    regex_condition_not_equals_less_than = r"([^=]*)<([\s\n]?[^=]*)"
    for ls in s:
        tes = ls
        if " ORDER BY " in ls:
            ls=ls.split(" ORDER BY ")[0]
        dict = {}
        # test = re.findall(regex_first_table, ls)
        if (len(re.findall(regex_first_table, ls)) > 0 and "=" not in ls):
            mat = re.findall(regex_first_table, ls)
            # print("testtttttttt", mat[0][0])
            if mat[0][0].count(".") == 3:
                # print(mat[0][0][0:mat[0][0].find(".")])
                dict["ConnectionStr"] = mat[0][0][0:mat[0][0].find(".")].strip()
                dict["Name"] = mat[0][0][mat[0][0].find(".") + 1:].strip()
            else:
                dict["ConnectionStr"] = conn
                dict["Name"] = mat[0][0].strip()
            dict["Alias"] = mat[0][1].strip() if mat[0][1].strip() != '' else None
            if check_duplicate_column(dict, table_result) == True:
                table_result.append(dict)
        elif (len(re.findall(regex_inner_join_table, ls)) > 0 and "=" not in ls):
            mat = re.findall(regex_inner_join_table, ls)
            if mat[0][0].count(".") == 3:
                # print(mat[0][0][0:mat[0][0].find(".")])
                dict["ConnectionStr"] = mat[0][0][0:mat[0][0].find(".")].strip()
                dict["Name"] = mat[0][0][mat[0][0].find(".") + 1:].strip()
            else:
                dict["ConnectionStr"] = conn
                dict["Name"] = mat[0][0].strip()
            dict["Alias"] = mat[0][1].strip() if mat[0][1].strip() != '' else None
            if check_duplicate_column(dict, table_result) == True:
                table_result.append(dict)

        elif is_check_column==True and len(re.findall(regex_condition, ls)) > 0 or len(re.findall(regex_condition_in, ls)) > 0 or len(
                re.findall(regex_condition_not_equals_a, ls)) > 0 or len(
                re.findall(regex_condition_not_equals_b, ls)) > 0 or len(
                re.findall(regex_condition_not_equals_greater_than, ls)) > 0 or len(
                re.findall(regex_condition_not_equals_less_than, ls)) > 0:
            if len(re.findall(regex_condition_not_equals_a, ls)) > 0:
                mat = re.findall(regex_condition_not_equals_a, ls)
            elif len(re.findall(regex_condition, ls)) > 0:
                mat = re.findall(regex_condition, ls)
            elif len(re.findall(regex_condition_in, ls)) > 0:
                mat = re.findall(regex_condition_in, ls)
            elif len(re.findall(regex_condition_not_equals_a, ls)) > 0:
                mat = re.findall(regex_condition_not_equals_a, ls)
            elif len(re.findall(regex_condition_not_equals_b, ls)) > 0:
                mat = re.findall(regex_condition_not_equals_b, ls)
            elif len(re.findall(regex_condition_not_equals_greater_than, ls)) > 0:
                mat = re.findall(regex_condition_not_equals_greater_than, ls)
            else:
                mat = re.findall(regex_condition_not_equals_less_than, ls)
            for con in mat[0]:
                dict = {}
                # if len(re.findall("\((.*?)\)",conn))==0:
                #     continue
                if con.strip().replace('.', '', 1).replace('(',"").replace('<','').replace('>','').isdigit() == False and con.__contains__(
                        "\'") == False and con.__contains__("()") == False and con.__contains__("#{") == False and len(
                    re.findall("\((.*?)\)", con)) == 0:
                    con_split = con.replace('(',"").replace('<','').replace('>','').split(".")
                    if len(con_split) == 2:
                        # dict["Type"] = "Condition"
                        dict["Name"] = con_split[1].strip()
                        dict["Alias"] = con_split[0].strip() if mat[0][1].strip() != '' else None
                        if check_duplicate_column(dict, conditon_result) == True:
                            conditon_result.append(dict)
                    elif len(con_split) == 1:
                        # dict["Type"] = "Condition"
                        dict["Name"] = con_split[0].strip()
                        dict["Alias"] = None
                        if check_duplicate_column(dict, conditon_result) == True:
                            conditon_result.append(dict)
    # print(split_result)
    result_dict["TableList"] = table_result
    result_dict["ConditionList"] = conditon_result
    result_dict["SqlStr"] = str
    return result_dict

class 发票CheckUpdateFormat():
    def __init__(self):
        pass
    def check_sql(self,entity,authinfo=None):
        try:
            mult_flag=False
            print("  step2.1 检查不允许包含注释符号")
            sql = entity["SqlStr"].replace("\nGO","\n").replace("\nGo","\n").replace("\ngo","\n").replace("\ngO","\n")
            #检查是否包含注释
            print("")
            if "--" in sql or "//*" in sql :
                raise Exception("SQL can't include /'--,/*/'")
            print("  step2.2 将传入的sql拆表检查")
            #先将表拆出

            url_sql_parser=r'https://dbsapi.newegg.org/dba/dbs/SQLParserForHint?format=json'
            if len(sql.strip())==0:
                raise Exception("SQL is null.")
            body={
                "TSQL":sql,
                 "ParserType":"SQL"
            }
            headers={"Accept": "application/json",
                     "ContentType":"application/json"}
            response=requests.post(url=url_sql_parser,data=body,headers=headers)
            if response.status_code!=200:
                raise Exception("SQL is incorrect.")
            split_list=json.loads(response.content)["BatchList"][0]["SQLScriptList"]
            error_list=[]
            result_list=[]
            for sql in split_list:
                #去掉多余的空格
                sql_upper = " ".join(sql.upper().split(" "))
                #检查是否存在临时表或者表变量
                # 检查表变量
                regix_table_variable=r'(?:DECLARE)+[\s\S][@a-zA-Z0-9_]*[\s\S]+(TABLE)'
                if len(re.findall(regix_table_variable, sql_upper)) > 0:
                    error_list.append("Don't support temp table or table variable")
                    continue
                #检查临时表
                regix_table_temp_create = r"(?:CREATE)+[\s\S]+(TABLE)+[\s\S][#a-zA-Z0-9_(]*"
                regix_table_temp_alter = r"(?:ALTER)+[\s\S]+(TABLE)+[\s\S][#a-zA-Z0-9_(]*"
                if len(re.findall(regix_table_temp_create, sql_upper)) > 0 or len(re.findall(regix_table_temp_alter, sql_upper)):
                    error_list.append("Don't support temp table or table variable")
                    continue
                #检查UPDATE DELETE语句必须包含Top
                regix_sp=r'(?:EXEC )+[a-zA-Z0-9_]*'
                #如果是SP，则跳过一切检查
                if len(re.findall(regix_sp,sql_upper))>0:
                    result_list.append({"SQL":sql_upper,"SqlType":"SP"})
                    continue
                #如果是UPDATE和DELETE，则需要检查是否包含top，且top的数量不能超过1000,如果是insert /delete /update则需要检查是否是源头表
                if "UPDATE " in sql_upper or "DELETE " in sql_upper  or "INSERT " in sql_upper:
                    mult_flag=True
                    regix_list_update=[]
                    if "UPDATE " in sql_upper or "DELETE " in sql_upper :
                        print("  step2.3 Update|Delete sql检查")
                        regix=r'(?:TOP)[\s\n]?\((?:[0-9]+)'
                        regix_list_update=re.findall(regix,sql_upper)
                        if len(regix_list_update)==0:
                            error_list.append("Sql must have top(?).")
                            continue
                        else:
                            digital=re.findall(r'[0-9]+',regix_list_update[0])
                            #为ruby提供查询超过1000的接口authinfo!="OTk5OTk5OTk5NjY2MjU0OTUyNzg5X3J3Mzg=CD"
                            if len(digital)>0 and int(digital[0])>1000 and authinfo!="OTk5OTk5OTk5NjY2MjU0OTUyNzg5X3J3Mzg=CD":
                                error_list.append("top(?) can't more than 1000.")
                                continue


                        result_list.append({"SQL": sql_upper, "SqlType": "SQLUpdate"})
                        print("  step2.3.1 Update|Delete ...检查是否是源头表")

                    if "INSERT " in sql_upper or len(regix_list_update) > 0 :
                        # 检查是否是源头表
                        # step 1 . 分离出表
                        url_sql_objectparser = r'https://dbsapi.newegg.org/dba/dbs/SQLParserForHint?format=json'
                        objectparser_body = {
                            "TSQL": sql,
                            "ParserType": "Object"
                        }
                        response = requests.post(url=url_sql_objectparser, data=objectparser_body, headers=headers)
                        object_parser = json.loads(response.content)
                        # 是否需要考虑返回多条数据的情况？？
                        if object_parser[0]["ObjType"] == 'Table' and object_parser[0]["ScriptType"] == 'Data Update':
                            if object_parser[0]["ServerName"] == '' or object_parser[0]["ServerName"] is None:
                                if object_parser[0]["DBName"] != "" and object_parser[0]["DBName"] != None:
                                    table_with_server = entity["ConnectionStr"] + "." + object_parser[0][
                                        "DBName"] + "." + object_parser[0]["ObjName"]
                                else:
                                    table_with_server = entity["ConnectionStr"] + "." + entity["DataBase"] + "." + \
                                                        object_parser[0]["ObjName"]
                            else:
                                table_with_server = object_parser[0]["ServerName"] + "." + object_parser[0][
                                    "DBName"] + "." + object_parser[0]["ObjName"]
                            # table_with_server
                            # step 2. 检查是否是源头表

                            url_sql_object_replication = r'https://apis.newegg.org/dba/rds/ObjReplicationQuery'
                            body_object_replication = {
                                "Env": entity["ExecuteEnv"],
                                "ObjectName": object_parser[0]["ObjName"]
                            }
                            response_object_replication = requests.post(url=url_sql_object_replication,
                                                                        data=body_object_replication, headers=headers)
                            if response.status_code!=200:
                                raise Exception("校验所操作的表是否是源头表失败")
                            result_object_replication = json.loads(response_object_replication.content)
                            check_replication = False
                            if len(result_object_replication["node"]) > 0:
                                for object in result_object_replication["node"]:
                                    # if "\\" in object["key"]:
                                    #     table_info=object["key"].split("\\")[1]
                                    # else:
                                    table_info = object["key"]
                                    if table_with_server.upper() == table_info.upper():
                                        check_replication = True
                                        result_list.append({"SQL": sql, "SqlType": "SQLInsert"})
                                        break
                            else:
                                check_replication = True
                                result_list.append({"SQL": sql, "SqlType": "SQLInsert"})
                            if check_replication == False:
                                error_list.append("table：%s isn't support edit|insert." % object_parser[0]["ObjName"])

                        else:
                            error_list.append("object is not a table or not ")
                            continue

                elif "SELECT " in sql_upper and 'FROM ' in  sql_upper :
                    print("  step2.3 Select sql检查是否包含top 和with nolock")
                    withnolock_list=["WITH(NOLOCK)","WITH (NOLOCK)"]
                    sql_with_nolock=False
                    for i in withnolock_list:
                        if i in sql_upper:
                            sql_with_nolock=True
                    regix = r'(?:TOP)[\s\n]?\(?(?:[0-9]+)'
                    regix_list = re.findall(regix, sql_upper)
                    if len(regix_list) == 0:
                        error_list.append("Sql must have top(?).")
                        continue
                    elif sql_with_nolock==False:
                        error_list.append("Sql must have WITH (NOLOCK).")
                    else:
                        digital = re.findall(r'[0-9]+', regix_list[0])
                        #为ruby调用提供特殊的token处理authinfo!="OTk5OTk5OTk5NjY2MjU0OTUyNzg5X3J3Mzg=CD"
                        if len(digital) > 0 and int(digital[0]) > 1000 and authinfo!="OTk5OTk5OTk5NjY2MjU0OTUyNzg5X3J3Mzg=CD":
                            error_list.append("top(?) can't more than 1000.")
                            continue

                        result_list.append({"SQL": sql_upper, "SqlType": "SQLSelect"})



                else:
                    result_list.append({"SQL": sql_upper, "SqlType": "Other"})
                    continue


            if len(error_list)>0:
                raise Exception(error_list)
            else:
                return {
                    "Error":None,
                    "SqlObject":result_list,
                    "MultFlag":mult_flag
                }
        except Exception as e:
            # if type(e.args[0])=="List":
            #     error_str=';\n'.join(error_list)
            raise Exception(e.args[0])