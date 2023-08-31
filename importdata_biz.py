from validator import validate, Required, Length, InstanceOf, In, Truthy
from app.business.user_validation import check_user_group
from .common import configcenter
from app.business.checksqlformat_biz import CheckFormat,call_exec_sql_v1
from app.business.execsql_biz import Execsql_biz
import requests,json,functools
from app.business.search_data_biz import  Search_Data_Biz
from app.business import authority
from app import db_conn
from app.utilities.decorator import row_dict
import copy,re

def validator_importdata(entity, type=None):
    validation = {
        "DataFrom": [Required, In(['GDEV', 'GQC','LDEV','PRD'])],
        "DataTo": [Required, In(['GDEV', 'GQC', 'LDEV', 'PRD'])],
        "SqlStr": [Required, InstanceOf(str)],
        "InUser": [Required, InstanceOf(str)],
    }

    check_validate_result = validate(validation, entity)
    if check_validate_result.valid is False:
        raise Exception(check_validate_result.errors)

def validator_importdatabyqueryid(entity, type=None):
    validation = {
        "DataFrom": [Required, In(['GDEV', 'GQC','LDEV','PRD'])],
        "DataTo": [Required, In(['GDEV', 'GQC', 'LDEV'])],
        "QueryID": [Required, InstanceOf(int)],
        "InUser": [Required, InstanceOf(str)],
    }

    check_validate_result = validate(validation, entity)
    if check_validate_result.valid is False:
        raise Exception(check_validate_result.errors)

def import_data_by_queryid(entity):
    try:
        #记录log
        EZ = Execsql_biz()
        EZ.exec_log(entity, entity["InUser"], "importdata")
        #获取queryinfo 信息
        queryinfo = get_inuser_and_type_and_query(entity["QueryID"])
        #获取创建query时选择的server
        original_server=queryinfo[0]["Server"]
        #导入的server，如果是从外部传入则用外部传入的server，否则使用创建时选择的server
        to_server=entity.get("Server",queryinfo[0]["Server"])
        #传入的server
        input_server=entity.get("Server",None)
        #传入的database
        database = entity.get("DataBase",None)
        #数据源的环境
        DataFrom=entity["DataFrom"]
        #数据导入的环境
        DataTo= entity["DataTo"]
        if DataFrom.upper() == DataTo.upper():
            raise ("DataFrom and DataTo can not same. ")
        if DataTo.upper() == 'PRD':
            raise ("Can not import to PRD. ")

        if queryinfo:
            #判定是否有权限进行导入
            checkresult=authority.judge_import_authority(queryinfo[0]["Type"],entity["InUser"],queryinfo[0]["GroupID"],DataFrom)
            if checkresult:
                #拆分导入的sql便于后面使用
                CF = CheckFormat()
                params = {
                    "DataFrom": DataFrom,
                    "DataTo": DataTo,
                    "SqlStr":queryinfo[0]["SqlStatement"],
                    # "ConnectionStr":to_server,
                    "ConnectionStr": original_server,
                    "InUser":entity["InUser"],
                    "DataBase":database
                }

                check_result = CF.check_select_sql_format(params, is_check_table=False,is_check_column=False)

                table_info, table_info_detail ,except_data= check_import_data(check_result, DataFrom, DataTo, input_server,database, original_server)
                result=import_data(params, check_result, table_info, table_info_detail)
                return True,result,except_data

                #如果查分成功则检查导入的sql是否符合要求
        #         if type(check_result)==list :
        #             check,data,table_info,table_info_detail=check_import_data(check_result, DataFrom, DataTo, input_server, database,original_server)
        #         else:
        #             raise Exception("Sql Check Failed,please check your sql[%s]"%queryinfo[0]["SqlStatement"])
        #
        #         if check:
        #             result=import_data(params,check_result,table_info,table_info_detail)
        #             return True,result
        #         else:
        #             return False,data
        # else:
        #     raise Exception("Queryid：%s  is invaild."%entity["QureryId"])


    except Exception as e:
        raise Exception(e.args[0])
def check_import_data(checkresult,DataFrom,DataTo,input_server,database,original_server):
    try:
        full_table_search_sql_list=[]
        full_table_info=[]
        full_except_data=[]
        for table_list in checkresult:
            original_sql=table_list['SqlStr']
            table_info_list=[]
            for table in table_list['TableList']:
                from_server=table["ConnectionStr"]
                if database is None:
                    current_database=table["Name"].split(".")[0]
                    tablename=table["Name"]
                else:
                    current_database=database
                    part_tablename_list=table["Name"].split(".")
                    part_tablename_list.pop(0)
                    tablename=current_database+"."+".".join(part_tablename_list)
                if input_server is not None:
                    to_server=input_server
                else:
                    to_server=table['ConnectionStr']
                alisa=table.get("Alias",None)
                get_table_column_priority_db = configcenter.sqlconfig.get("get_table_column_priority") % (
                    current_database, tablename)
                table_info_list.append(
                    get_db_info(get_table_column_priority_db, from_server, to_server, DataFrom, DataTo, tablename,
                                alisa))
                # #获取导入表的表结构， 这里做了特殊处理，因为prd目前没有办法拿到表结构，所以如果遇到prd的时候就从导入的目标环境获取数据的表接口
                # if DataFrom.upper()!="PRD":
                #     table_info_list.append(get_db_info(get_table_column_priority_db,from_server,to_server,DataFrom,DataTo,tablename,alisa))
                # else:
                #     table_info_list.append(get_db_info(get_table_column_priority_db,from_server,to_server,DataFrom,DataTo,tablename,alisa))
            #生成查询sql，并返回结果
            all_table_search_sql_list,all_table_info,except_data=generate_search_target_table_sql(original_sql,table_info_list,DataFrom,original_server,DataTo)
            full_table_search_sql_list+=all_table_search_sql_list
            full_table_info+=all_table_info
            full_except_data+=except_data
        return full_table_search_sql_list,full_table_info,full_except_data
    except Exception as e:
        raise Exception(e.args[0])


def check_target_data(all_table_search_sql_list,to_env,orignal_server):
    conflict_data_list=[]
    count=0
    for s in all_table_search_sql_list:

        to_server=s["to_server"]
        conflict_data=[]
        # #验证查询sql是否有冲突
        # original_search_result=get_data(s["original_search_sql"],orignal_server,to_env)
        # if len(original_search_result["data"])>0:
        #     conflict_data=original_search_result["data"]
        #验证主键是否有冲突
        if s["pk_search_sql"] is not None:
            pk_search_result = get_data(s["pk_search_sql"], to_server, to_env)
            if pk_search_result["IsSuccess"]==True :
                if  len(pk_search_result["data"])>0:
                    delete_conditon_list = []
                    for data in pk_search_result["data"]:
                        conflict_data.append(data)
                        delete_data_list=[]
                        for pk in s['pk_list']:
                            delete_data_list.append(pk['pk_colomn_name']+"<> '"+data[pk['pk_colomn_name']]+"'")
                        delete_conditon_list.append("("+" AND ".join(delete_data_list)+")")
                    delete_conditon_list_str=" (  "+" OR".join(delete_conditon_list)+" )"
                    if count==0:
                        if " WHERE " in s["pk_search_sql"]:
                            pk_search_sql_list=s["pk_search_sql"].split(" WHERE ",1)
                            s["pk_search_sql"]=pk_search_sql_list[0]+ " WHERE "+delete_conditon_list_str+" AND "+pk_search_sql_list[1]
                        elif "GROUP BY " in s["pk_search_sql"]:
                            pk_search_sql_list = s["pk_search_sql"].split("GROUP BY ", 1)
                            s["pk_search_sql"] = pk_search_sql_list[0] + delete_conditon_list_str + " GROUP BY " + pk_search_sql_list[1]

                    else:
                        if " WHERE " in all_table_search_sql_list[0]["pk_search_sql"]:
                            pk_search_sql_list=all_table_search_sql_list[0]["pk_search_sql"].split(" WHERE ",1)
                            all_table_search_sql_list[0]["pk_search_sql"]=pk_search_sql_list[0]+ " WHERE "+delete_conditon_list_str+" AND "+pk_search_sql_list[1]
                        elif "GROUP BY " in s["pk_search_sql"]:
                            pk_search_sql_list = all_table_search_sql_list[0]["pk_search_sql"].split("GROUP BY ", 1)
                            all_table_search_sql_list[0]["pk_search_sql"] = pk_search_sql_list[0] + delete_conditon_list_str + " GROUP BY " + pk_search_sql_list[1]

                else:
                    conflict_data=[]
            else:
                raise Exception("Exec sql failed[1].")
        if len(conflict_data)>0:

            distinct_conflict_data = []
            [distinct_conflict_data.append(r) for r in conflict_data if r not in distinct_conflict_data]
            conflict_data_list.append({"TableName":s["TableName"],"ConflictData":distinct_conflict_data})
            if count == 0:
                if " WHERE " in s["pk_search_sql"]:
                    new_sql_list=s["pk_search_sql"].split()

        else:
            continue

        count+=1
    return  conflict_data_list



#生成目标数据
def generate_search_target_table_sql(original_sql,table_info_list,DataFrom,server,DataTo):
    global originalsql
    i = 1
    originalsql=original_sql
    conflict_data=[]
    count_while=0
    while i==1:
        count_while+=1
        i = 0
        if count_while>50:
            raise Exception("With your import sql over 50 data is PK conflict, Please check your sql. ")

        #todo
        original_sql_list=original_sql.upper().split(" FROM ",1)
        #获取top的值
        regex_first_table = r"\d+\.?\d*"
        sql_top_number_list = re.findall(regex_first_table, original_sql_list[0])

        if len(sql_top_number_list)>0:
            sql_top_number = sql_top_number_list[0]
            if int(sql_top_number) > 1000:
                    sql_top_number = "1000"
        else:
            sql_top_number = "1000"
        count=0
        first_table_pk_condition_str=""


        search_sql_list = []
        table_list_info_list = []
        for table_info in table_info_list:
            coloum_str_for_insert = ",".join(f"[{i}]"for i in table_info["column_list"])
            alias=table_info["alias"]
            if alias!=None:
                column_list_new=[]
                for column in table_info["column_list"]:
                    column_list_new.append(alias+"."+column)
                coloum_str=",".join(column_list_new)
                if count==0:
                    first_table_pk_condition=table_info['pk_list']
                    sql="SELECT TOP("+sql_top_number+")"+coloum_str+" FROM "+original_sql_list[1]

                else:
                    sql="SELECT TOP(1000)" + coloum_str + " FROM " + original_sql_list[1]
                    if len(first_table_pk_condition_str) > 0:
                        if " WHERE " in sql:
                            sql_list=sql.split(" WHERE ")
                            sql=sql_list[0]+" WHERE " +first_table_pk_condition_str +" AND " +sql_list[1]
                            # sql += " AND " + first_table_pk_condition
                        elif " GROUP BY " in sql:
                            sql_list = sql.split(" GROUP BY ")
                            sql = sql_list[0] + " WHERE " + first_table_pk_condition_str + " GROUP BY " + sql_list[1]
                        elif " ORDER BY " in sql:
                            sql_list = sql.upper().split(" ORDER BY ")
                            sql = sql_list[0] + " WHERE " + first_table_pk_condition_str + " ORDER BY " + sql_list[1]
                        else:
                            sql += " WHERE " + first_table_pk_condition_str
                distinct_pk_list = []
                [distinct_pk_list.append(r) for r in table_info["pk_list"] if r not in distinct_pk_list]
                data_from_result = get_data(sql, server, DataFrom)
            else:
                coloum_str = ",".join(f"[{i}]" for i in table_info["column_list"])
                if count==0:
                    sql="SELECT TOP("+sql_top_number+")"+coloum_str +" FROM "+original_sql_list[1]
                else:
                    sql = "SELECT TOP(1000)" + coloum_str + " FROM " + original_sql_list[1]
                    if len(first_table_pk_condition_str) > 0:
                        pk_sql += " AND (" + first_table_pk_condition_str +")"
                distinct_pk_list = []
                [distinct_pk_list.append(r) for r in table_info["pk_list"] if r not in distinct_pk_list]
                try:
                    data_from_result = get_data(sql, server, DataFrom)
                except Exception as e:
                    raise Exception("sql exec failed,sql is: %s"%sql)


            if data_from_result["IsSuccess"]==True:
                distinct_data_from_result = []
                [distinct_data_from_result.append(r) for r in data_from_result["data"] if r not in distinct_data_from_result]
            if len(table_info["pk_list"])>0:
                if len(distinct_data_from_result)>0:
                    is_identity=0
                    for item in distinct_pk_list:
                        if item['column_identity']==1 or item['column_identity']=='1':
                            is_identity=1

                    final_where_condition_list=[]
                    final_where_condition_with_alias=[]
                    for data in distinct_data_from_result:
                        where_condition=[]
                        where_condition_with_alias = []
                        for pk in distinct_pk_list:
                            where_condition.append(pk["pk_colomn_name"]+"='"+str(data.get(pk["pk_colomn_name"],None) if data.get(pk["pk_colomn_name"],None)!=None else data.get(pk["pk_colomn_name"].upper()))+"'")
                            if alias!=None:
                                where_condition_with_alias.append(alias+"."+pk["pk_colomn_name"]+"='"+str(data.get(pk["pk_colomn_name"],None) if data.get(pk["pk_colomn_name"],None)!=None else data.get(pk["pk_colomn_name"].upper()))+"'")
                        final_where_condition_list.append("("+" AND ".join(where_condition)+")")
                        if len(where_condition_with_alias)>0:
                            final_where_condition_with_alias.append("("+" AND ".join(where_condition_with_alias)+")")
                        else:
                            final_where_condition_with_alias=final_where_condition_list
                    final_where_condition=" OR ".join(final_where_condition_list)
                    final_where_condition_with_alias_str=" OR ".join(final_where_condition_with_alias)
                    if count==0:
                        first_table_pk_condition_str+=final_where_condition_with_alias_str
                    pk_sql="SELECT top(1000) * FROM " +table_info["table_name"] +" WITH(NOLOCK) WHERE "+final_where_condition
                    try:
                        data_to_result=get_data(pk_sql,table_info['to_server'],DataTo)
                        # if count==0:
                        #     data_to_result_for_orginal_sql=get_data(sql,table_info['to_server'],DataTo)
                    except Exception as e:
                        raise Exception("sql exec failed,sql is: %s"%pk_sql)
                    if data_to_result["IsSuccess"]==True :
                        distinct_data_to_result = []
                        [distinct_data_to_result.append(r) for r in data_to_result["data"] if r not in distinct_data_to_result]
                        # [distinct_data_to_result.append(r) for r in data_to_result_for_orginal_sql["data"] if r not in distinct_data_to_result]
                        if len(distinct_data_to_result) > 0:
                            conflict_data.append( {"table_name":table_info["table_name"],"pk_list":distinct_pk_list,"distinct_data":distinct_data_to_result})

                            delete_conditon_list = []
                            delete_where_list=[]
                            for data in distinct_data_to_result:
                                delete_conditon_list.append(data)
                                delete_data_list = []
                                for pk in distinct_pk_list:
                                    if table_info['alias']!="" and table_info['alias']!=None:
                                        delete_data_list.append(table_info['alias']+"."+
                                            pk['pk_colomn_name'] + "<> '" + str(data[pk['pk_colomn_name']]) + "'")
                                    else:
                                        delete_data_list.append(pk['pk_colomn_name'] + "<> '" + str(data[pk['pk_colomn_name']]) + "'")
                                delete_where_list.append("(" + " AND ".join(delete_data_list) + ")")
                            delete_where_list_str = " (  " + " AND".join(delete_where_list) + " )"

                            if "WHERE " in original_sql.upper():
                                sql_list = original_sql.upper().split("WHERE ")
                                original_sql= sql_list[0] + " WHERE " + delete_where_list_str + " AND " + sql_list[1]
                                # sql += " AND " + first_table_pk_condition
                            elif " GROUP BY " in original_sql.upper():
                                sql_list = original_sql.upper().split(" GROUP BY ")
                                original_sql = sql_list[0] + " WHERE " + delete_where_list_str + " GROUP BY " + sql_list[1]
                            elif " ORDER BY " in original_sql.upper():
                                sql_list = original_sql.upper().split(" ORDER BY ")
                                original_sql = sql_list[0] + " WHERE " + delete_where_list_str + " ORDER BY " + sql_list[1]
                            else:
                                original_sql += " WHERE " + delete_where_list_str

                            #这里跳过重复的key
                            i= 1
                            break
                        else:
                            distinct_data_to_result = []


                    table_search_list={"TableName":table_info["table_name"],"original_search_sql":sql,"pk_search_sql":pk_sql}
                    table_list_info = {"TableName": table_info["table_name"], "original_search_sql": sql,
                                         "pk_search_sql": pk_sql, "column_data_list": table_info['column_list'],
                                       "from_data": distinct_data_from_result,"is_identity":is_identity,"to_server":table_info['to_server'],"coloum_str":coloum_str_for_insert,"pk_list":distinct_pk_list,"alias":table_info["alias"]}
                    table_list_info_list.append(table_list_info)
                    search_sql_list.append(table_search_list)

                else:
                    is_identity = 0
                    for item in distinct_pk_list:
                        if item['column_identity'] == 1:
                            is_identity = 1
                    table_search_list = {"TableName": table_info["table_name"], "original_search_sql": sql,
                                         "pk_search_sql": None}
                    table_list_info = {"TableName": table_info["table_name"], "original_search_sql": sql,
                                       "pk_search_sql": None, "column_data_list": table_info['column_list'],
                                       "from_data": distinct_data_from_result,"is_identity":is_identity,"to_server":table_info['to_server'],"coloum_str":coloum_str_for_insert,"pk_list":distinct_pk_list,"alias":table_info["alias"]}
                    table_list_info_list.append(table_list_info)
                    search_sql_list.append(table_search_list)

            else:
                is_identity = 0
                for item in distinct_pk_list:
                    if item['column_identity'] == 1:
                        is_identity = 1
                table_search_list = {"TableName": table_info["table_name"], "original_search_sql": sql,
                                     "pk_search_sql": None}
                table_list_info={"TableName": table_info["table_name"], "original_search_sql": sql,
                                     "pk_search_sql": None,"column_data_list":table_info['column_list'],"from_data":distinct_data_from_result,"is_identity":is_identity,"to_server":table_info['to_server'],"coloum_str":coloum_str_for_insert,"pk_list":distinct_pk_list,"alias":table_info["alias"]}
                table_list_info_list.append(table_list_info)
                search_sql_list.append(table_search_list)

            count += 1

    return search_sql_list, table_list_info_list,conflict_data







#执行sql获取数据
def get_data(sql,server,env):
    EZ=Execsql_biz()
    ent = {
        "executeenv": env,
        "sqlstr": sql,
        "connectionstr": server
    }
    try:
        table_column_info = EZ.control_exec_api(ent,isauth=False)
    except Exception as e:
        raise(e.args[0])
    return table_column_info

def get_db_info(sql,from_server,to_server,from_env,to_env,tablename,alias):
    EZ=Execsql_biz()
    if from_env.upper()!="PRD":
        ent = {
            "executeenv": from_env,
            "sqlstr": sql,
            "connectionstr": from_server
        }
    else:
        ent = {
            "executeenv": from_env,
            "sqlstr": sql,
            "connectionstr": from_server
        }
        # ent = {
        #     "executeenv": to_env,
        #     "sqlstr": sql,
        #     "connectionstr": to_server
        # }
    table_column_info = EZ.control_exec_api(ent,isauth=False)
    column_pk_list=[]
    column_list=[]
    for data in table_column_info["data"]:
        if data["ColumnComputed"]!=1:
            column_list.append(data["ColumnName"])
        if data["ColumnPK"]:
            column_pk = {"pk_colomn_name": data["ColumnName"], "pk_type": data["ColumnType"],
                         "column_identity": data["ColumnIdentity"],"column_length":data["ColumnLength"],"column_ColumnScale":data["ColumnScale"]}
            column_pk_list.append(column_pk)
        if data["ColumnIdentity"] == 1 or data["ColumnIdentity"] == "1":
            column_pk = {"pk_colomn_name": data["ColumnName"], "pk_type": data["ColumnType"],
                         "column_identity": data["ColumnIdentity"],"column_length":data["ColumnLength"],"column_ColumnScale":data["ColumnScale"]}
            column_pk_list.append(column_pk)
    table_column_info = {"columns": table_column_info["data"], "pk_list": column_pk_list,"table_name":tablename,"alias":alias,"column_list":column_list,"to_server":to_server }
    return table_column_info

@row_dict
def get_inuser_and_type_and_query(queryid):
        get_queryid_info_sql = configcenter.searchdataconfig.get("get_queryid_info_by_id") % str(queryid)
        r = db_conn.execute(get_queryid_info_sql)
        return r

def import_data(entity,check_result,table_info,table_info_detail):

    import_result_list=[]
    data_to=entity["DataTo"]
    data_from=entity["DataFrom"]
    # to_server=entity['ConnectionStr']
    database=entity['DataBase']

    for table in table_info_detail:
        TableName = table['TableName']
        insert_data = table['from_data']
        to_server=table["to_server"]
        #根据传入的database重新计算tableName
        if database is not None:
            table_database_to = database.upper()
            table_name_split = TableName.split(".")
            table_name_to = table_database_to + "." + table_name_split[1] + "." + table_name_split[2]
        else:
            table_name_to=TableName

        #如果导入的数据不为空则计算导入的sql，否则导入的sql为空
        if insert_data is not None:
            data_count=len(table['from_data'])
        else:
            data_count=0
        if data_count>0:
            columnlist=table['column_data_list']
            # columnlist_to_str='('+",".join(columnlist)+')'
            data_list=[]
            count=0
            for data in table['from_data']:
                if count==0:
                    columnlist = d = [key for key, value in data.items()]
                    # columnlist_to_str='('+",".join(columnlist)+')'
                    d = [value.replace("'","*") if type(value)== str else value  for key, value in data.items()]
                    str_d = "(" + str(d)[1:-1] + ")"
                    data_list.append(str_d.replace(', \"',',\'').replace('\",',"\',") if ', \"' in str_d else str_d)
                else:
                    d=[value.replace("'","*") if type(value)==str else value for key,value in data.items()]
                    str_d = "(" + str(d)[1:-1] + ")"
                    data_list.append(str_d.replace(', \"', ',\'').replace('\",', "\',") if ', \"' in str_d else str_d)
                count+=1
            data_list_to_str=",".join(data_list)
            if table['is_identity']==1:
                orgin_sql = "SET IDENTITY_INSERT "+TableName+" ON;INSERT INTO " + TableName + "("+table['coloum_str']+")" + " VALUES " + data_list_to_str +"SET IDENTITY_INSERT "+TableName+" OFF;"
            else:
                orgin_sql="INSERT INTO "+TableName+ "("+table['coloum_str']+")" + " VALUES "+data_list_to_str
            sql=orgin_sql.replace("None","null").replace("False","0").replace("True","1").replace("\'_&Null&_\'","null").replace("***","1")
            print(sql)
            import_result_list.append({"Table_Name": table_name_to, "Count": data_count, "ImportSQL": sql,"to_server":to_server})
        else:
            import_result_list.append({"Table_Name":table_name_to,"Count":data_count,"ImportSQL":None,"to_server":to_server})
            continue
    for sql_info in import_result_list:
        try:
            if sql_info["ImportSQL"] is not None:
                result=get_data(sql_info["ImportSQL"], sql_info["to_server"], data_to)
                if result["IsSuccess"]==True:
                    sql_info.update({"IsSuccess":True})
                else:
                    sql_info.update({"IsSuccess": False})
            else:
                sql_info.update({"IsSuccess": True})

        except Exception as e:
            sql_info.update({"IsSuccess": False})
            return import_result_list
    return import_result_list




    #
    # first_table_alisa=""
    # for li in check_result:
    #     i = 0
    #
    #     # 组装最后查询各个表的sql：final_sql_list（list）
    #     final_sql_list = []
    #     sql=li["SqlStr"].upper()
    #     pk = []
    #     temp_table=""
    #     first_table_select_pk_sql=""
    #     if len(li["TableList"]):
    #         for table in li["TableList"]:
    #             # first_table=li["TableList"][0]
    #             if entity["DataBase"] is not None:
    #                 table_name=table["Name"]
    #
    #                 table_database_to = entity["DataBase"].upper()
    #                 table_name_split=table["Name"].split(".")
    #                 table_database =table_name_split[0]
    #                 table_name_to=table_database_to+"."+table_name_split[1]+"."+table_name_split[2]
    #                 table_conn_to =entity["ConnectionStr"]
    #
    #             else:
    #                 table_name = table["Name"]
    #                 table_name_to = table["Name"]
    #                 table_database=table_name.split(".")[0]
    #                 table_conn_to=table["ConnectionStr"]
    #                 table_database_to=table_database
    #                 # table_name_to =table_name.split(".")[0]
    #
    #             table_conn=table["ConnectionStr"]
    #
    #             table_alias=table["Alias"]
    #
    #
    #             #对第一张表进行查询获取pk （list）
    #             #组装后边关联表需要的临时表temp_table （string）
    #             #获取第一张表的查询sql
    #             if i == 0:
    #                 # first_table_alisa+=table["Alias"] if table["Alias"] is not None else ""
    #                 # get_table_column_priority_db = configcenter.sqlconfig.get("get_table_column_priority") % (
    #                 # table_database_to, table_name_to)
    #                 # ent = {
    #                 #     "executeenv": data_to,
    #                 #     "sqlstr": get_table_column_priority_db,
    #                 #     "connectionstr": table_conn_to
    #                 # }
    #                 # checkresult = EZ.control_exec_api(ent)
    #                 # for d in checkresult["data"]:
    #                 #     if d["ColumnPK"]=='PK':
    #                 #         pk.append({"ColumnName":d['ColumnName'],"ColumnType":d['ColumnType'],"ColumnLength":d["ColumnLength"],"ColumnScale":d["ColumnScale"]})
    #                 # run_function = lambda x, y: x if y in x else x + [y]
    #                 # drop_duplicate_pk = functools.reduce(run_function, [[], ] + pk)
    #                 for tab in table_info:
    #                     if tab["table_name"]==table_name_to:
    #                         drop_duplicate_pk=tab["pk_list"]
    #                 temp_table += generate_temp_table_sql(first_table_select_pk_sql, drop_duplicate_pk)
    #                 first_talbel_formatsql=get_sqlstr_by_alias(table,sql,i)
    #                 first_table_select_pk_sql+=get_sqlstr_by_pk(pk,table,sql)
    #                 first_talbel_formatsql=process_first_table_sql(first_talbel_formatsql,pk,first_table_alisa,data_from,table_conn)
    #                 final_sql_list.append({"TableName":table_name_to,"SearchSql":first_talbel_formatsql,"Server":table_conn_to})
    #
    #
    #             #根据第一张表获取的临时表temp_table与后面的表进行sql拼装，并且生成除第一张表外的其他表的查询sql
    #             else:
    #
    #                 #说明：final_sql_list[0]["SearchSql"]是导入的第一张表组合组件后的sql，后面的表只需要将别名替换掉即可
    #                 talbel_formatsql = get_sqlstr_by_alias(table, final_sql_list[0]["SearchSql"],i)
    #                 final_sql_list.append({"TableName": table_name_to, "SearchSql": talbel_formatsql, "Server": table_conn_to})
    #                 # temp_table_list=talbel_formatsql.split("WHERE",1)
    #                 #
    #                 # temp_table_list[0]+="INNER JOIN @temp_table temp_table ON "
    #                 # count=0
    #                 # for p in pk:
    #                 #     if count==0:
    #                 #         temp_table_list[0]+=first_table_alisa+"."+p["ColumnName"]+"= temp_table."+p["ColumnName"]
    #                 #     else:
    #                 #         temp_table_list[0]+= "AND "+first_table_alisa + "." + p["ColumnName"] + "= temp_table." + p["ColumnName"]
    #                 #     count+=1
    #                 # temp_table_join=" WHERE".join(temp_table_list)
    #                 # final_sql=temp_table+temp_table_join
    #                 #
    #                 # final_sql_list.append({"TableName":table_name_to,"SearchSql":final_sql,"Server": table_conn})
    #
    #             i += 1
    #
    #         # target_table_sql_lsit=[]
    #         # copy_finalsql_list=copy.deepcopy(final_sql_list)
    #         # for sql in copy_finalsql_list:
    #         #     #如果查询的server和导入的server不一致的情况，直接去掉sql中存在server的字样
    #         #     if table_conn!=table_conn_to:
    #         #         sql["SearchSql"]=sql["SearchSql"].replace(table_conn+".","")
    #         #     #如果查询的database和导入的database不一致情况，需要替换成导入的database，方便用于验证目标表是否有相同的数据
    #         #     if table_database!=table_database_to:
    #         #         sql["SearchSql"] = sql["SearchSql"].replace(table_database + ".", table_database_to+".")
    #         #     # if " WHERE " not in sql["SearchSql"]:
    #         #         # sql["SearchSql"]= process_target_sql(sql["SearchSql"],data_from,data_to,table_conn,table_conn_to,)
    #         #     target_table_sql_lsit.append(sql)
    #         # check_target_env_data=check_data_from_target_env(target_table_sql_lsit,data_to,table_conn_to)
    #         # if (len(check_target_env_data)>0):
    #         #     return {"IsSuccess":False,"Reason":"Data conflict exists","Detail": check_target_env_data}
    #         exec_result = select_import_data(final_sql_list, data_from, data_to, table_database_to)



    #     else:
    #         raise Exception("Table is not exists")
    # # getprimarykey
    # return exec_result
def process_first_table_sql(first_talbel_formatsql,pk,first_table_alisa,from_env,from_server):
    EZ = Execsql_biz()
    ent = {
        "executeenv": from_env,
        "sqlstr": first_talbel_formatsql,
        "connectionstr": from_server
    }
    result = EZ.control_exec_api(ent,isauth=False)

    join_list = []
    if first_table_alisa!="":
        for i in result["data"]:
            count = 0
            join_sql=""
            for n in pk:
                if count==0:
                  join_sql+="("+first_table_alisa+"."+n["ColumnName"]+"="+str(i[n["ColumnName"]])
                else:
                  join_sql += "AND " + first_table_alisa + "." + n["ColumnName"] + "=" + str(i[n["ColumnName"]])
                count += 1
            join_sql+=")"
            join_list.append(join_sql)
    else:
        for i in result["data"]:
            count = 0
            join_sql = ""
            for n in pk:
                if count==0:
                  join_sql+="("+n["ColumnName"]+"="+i[n["ColumnName"]]
                else:
                  join_sql += "AND " + "." + n["ColumnName"] + "=" + i[n["ColumnName"]]
                count += 1
            join_sql+=")"
            join_list.append(join_sql)
    final_join_str=" OR ".join(join_list)
    first_talbel_formatsql_split=first_talbel_formatsql.split(" WHERE ",1)
    if len(first_talbel_formatsql_split)==1:
        final_sql=first_talbel_formatsql_split[0]+" WHERE " +final_join_str
    else:
        final_sql=first_talbel_formatsql_split[0]+" WHERE " +first_talbel_formatsql_split[1]+" AND (" +final_join_str +" )"
    return final_sql


# def process_target_sql(sql,data_from,data_to):
#     entity = {
#         "executeenv": data_to,
#         "sqlstr": sql,
#         "connectionstr": tab["Server"]
#     }
#     return sql

def check_data_from_target_env(tablelist,data_to,conn_to):
    EZ=Execsql_biz()
    result=[]
    for tab in tablelist:
            entity = {
                "executeenv": data_to,
                "sqlstr": tab["SearchSql"],
                "connectionstr": conn_to
            }
            checkresult = EZ.control_exec_api(entity,isauth=False)
            run_function = lambda x, y: x if y in x else x + [y]
            drop_duplicate_pk = functools.reduce(run_function, [[], ] + checkresult["data"])
            if len(drop_duplicate_pk)>0:
                result.append({"TableName":tab["TableName"],"ConflictCount":len(drop_duplicate_pk),"Data":drop_duplicate_pk})

    return result

def select_import_data(checked_final_sql,datafrom,datato,table_database):
    EZ = Execsql_biz()
    return_result=[]
    for item in checked_final_sql:
        entity = {
            "executeenv":datafrom,
            "sqlstr": item["SearchSql"],
            "connectionstr": item['Server']
        }
        res=EZ.control_exec_api(entity,isauth=False)
        data_sql=""
        count_0=0
        # delete_sql=get_delete_sql(entity,table_name)
        list_keys = list(res["data"][0].keys())
        str_keys="(" + ",".join(list_keys) + ")"
        run_function = lambda x, y: x if y in x else x + [y]
        drop_duplicate_list = functools.reduce(run_function, [[], ] + res["data"])
        # drop_duplicate_list =list(set(res["data"]))
        for data in drop_duplicate_list:
            list_data=list(data.values())
            if count_0==0:
                data_sql+="("+str(list_data)[1:-1]+")"
            else:
                data_sql += ",(" + str(list_data)[1:-1] + ")"
            count_0+=1
        if len(data_sql)>0:
            first_part_sql = "insert into %s %s values"%(item['TableName'],str_keys)+data_sql
            total_sql="""
            use %s
            if exists(select   top 1 1
                  from syscolumns T1
             where OBJECT_ID('%s')=T1.id and columnproperty(T1.id, T1.name, 'IsIdentity')=1)
             BEGIN
                SET IDENTITY_INSERT %s ON;
                %s;
                 SET IDENTITY_INSERT  %s OFF;
            END
            else
            BEGIN
                %s
            END
            """%(table_database,item['TableName'],item['TableName'],first_part_sql,item['TableName'],first_part_sql)

            reuslt_sql=total_sql.replace("None","null").replace("False","0").replace("True","1").replace("\'_&Null&_\'","null").replace("***","1")
            entity = {
                "executeenv": datato,
                "sqlstr": reuslt_sql,
                "connectionstr": item['Server']
            }
            resp = EZ.control_exec_api(entity,isauth=False)
            return_result.append({
                "IsSuccess": True,
                "TableName": item['TableName'],
                "EffectCount": len(drop_duplicate_list)
            })
        else:
            return_result.append({
                "IsSuccess":True,
                "TableName":item['TableName'],
                "EffectCount": len(drop_duplicate_list)
            })
        # if result
    return return_result





def get_sqlstr_by_alias(entity,sql,count):
    split_result=sql.split("FROM",1)
    if  count==0 and " TOP" in sql:
        regex_first_table = r"\d+\.?\d*"
        sql_top_number_list = re.findall(regex_first_table, sql)
        # sql_top_number = [temp for temp in split_result[0].split() if temp.isdigit()][0]
        sql_top_number=sql_top_number_list[0]
        if int(sql_top_number) >1000:
            sql_top_number = "1000"
    else:
        sql_top_number="1000"



    if entity["Alias"]!=None:
        result_sql= "SELECT TOP ("+sql_top_number+") " +entity["Alias"]+".* FROM"+split_result[1]
    else:
        result_sql = "SELECT TOP (" + sql_top_number + ") " + " * FROM" + split_result[1]

    return result_sql

# def get_delete_sql(entity,table_name):
#     pre_deletesql="""
#     declare @pk_coloumn int
#  select @pk_coloumn=T1.name   from syscolumns T1 left join
#       (
#           SELECT syscolumns.id as id,syscolumns.colid as colid
#           FROM syscolumns,sysobjects,sysindexes,sysindexkeys
#           WHERE sysobjects.xtype = 'PK'
#           AND sysobjects.parent_obj = syscolumns.id
#           AND sysindexes.id = syscolumns.id
#           AND sysobjects.name = sysindexes.name
#           AND sysindexkeys.id = syscolumns.id
#           AND sysindexkeys.indid = sysindexes.indid
#           AND syscolumns.colid = sysindexkeys.colid
#       ) T3  on T1.id=T3.id and T1.colid=T3.colid
# 	  where OBJECT_ID('nsls.dbo.newegg_sotransaction')=T1.id and T3.id is not null"""
#     sql=entity["sqlstr"].replace(".*","")
#     return None

def get_sqlstr_by_pk(pk,entity,sql):
    split_result = sql.split("FROM", 1)
    sql_top_number = [temp for temp in split_result[0].split() if temp.isdigit()][0]
    if len(sql_top_number) == 0:
        sql_top_number = 1
    count=0
    selectcolumn=""
    if entity["Alias"]!=None:
        for p in pk:
            if count==0:
                selectcolumn+=entity["Alias"] + "."+p["ColumnName"]
            else:
                selectcolumn += ","+entity["Alias"] + "." + p["ColumnName"]
            count+=1
    else:
        for p in pk:
            if count==0:
                selectcolumn+=p["ColumnName"]
            else:
                selectcolumn += ","+ p["ColumnName"]
            count+=1
    result_sql = "SELECT TOP (" + sql_top_number + ") " + selectcolumn+ " FROM" + split_result[1]
    return result_sql

def generate_temp_table_sql(select_pk_sql,pk):
    insert_data_to_db_list = []
    # sql_str="Declare @temp Table (%s)"
    temp_table_columns = []
    for column in pk:
        column_type = column["pk_type"]
        column_name = column["pk_colomn_name"]
        column_length = column["column_length"]
        if "char" in column["pk_type"]:
            if str(column["column_length"]) == "-1":
                column_length = 'max'
            temp_table_column = "[%s] %s (%s)" % (column_name, column_type, column_length)
        elif "text" in column["pk_type"]:
            temp_table_column = "[%s] %s (%s)" % (column_name, "nvarchar", 4000)
        elif "decimal" == column["pk_type"]:
            temp_table_column = "[%s] %s(%s,%s)" % (
                column_name, column_type, column["column_length"], column["column_ColumnScale"])
        elif "nvarchar" == column_type and column_length == '-1':
            temp_table_column = "[%s] nvarchar (max)" % column_name
        else:
            temp_table_column = "[%s] %s" % (column_name, column_type)
        temp_table_columns.append(temp_table_column)

        insert_clouddata_to_db = "insert into @temp_table %s" % select_pk_sql
        insert_data_to_db_list.append(insert_clouddata_to_db)
    # insert_data_to_db_list_sql=','.join(temp_table_columns)

    temp_table_columns_info = ','.join(temp_table_columns)
    declare_temp_table = "declare @temp_table Table (%s);" % temp_table_columns_info
    insert_cloud_to_db_sql = ";".join(insert_data_to_db_list) + ";"
    final_sql = declare_temp_table + insert_cloud_to_db_sql
    return final_sql





