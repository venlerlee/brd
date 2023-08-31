import json
from .common import configcenter
from app import db, db_conn, db_engine
import datetime
from app.decorators.Decorator import row_dict
import requests
from app.business.searchTagImpl import SearchTagData
from app.business.task_search_biz import Task_Search_biz
from sqlalchemy.orm import sessionmaker
from app.business.autoparameters_biz import AutoCreateParameters_biz
import copy
import uuid
from app.business.showscene_business import ShowScene

class SearchScenes(object):
    def __init__(self, scene_id, type, group_id, user_id, product_id, scene_name, FilterNoMap, keywords, search_tags, category, iscombine, pageindex, pagesize, owner, isfavorite,description):
        scenes_result = SearchScenes.get_scenes(scene_id, type, group_id, user_id, product_id, scene_name, FilterNoMap,
                                                keywords, search_tags, category, iscombine, pageindex, pagesize, owner, isfavorite,description)
        self.total = scenes_result["TotalCount"]
        self.data = scenes_result["Data"]
        self.pageindex = scenes_result["PageIndex"]
        self.pagesize = scenes_result["PageSize"]
        all_step_info = self.get_all_step()
        self.data = self.add_step_for_scene(self.data, all_step_info)
        print(all_step_info)

    @classmethod
    def get_scenes(cls, scene_id, type, group_id, user_id, product_id, scene_name, FilterNoMap, keywords, search_tags, category, iscombine, pageindex, pagesize, owner, isfavorite,description):
        if isfavorite is not None and isfavorite == 'true':
            convert_isfavorite= 1
        else:
            convert_isfavorite = None

        par_scenes = {"SceneID": scene_id, "Type": type, "GroupID": group_id, "UserID": user_id,"ProductID": product_id,
                      "SceneName": scene_name, "FilterNoMap": FilterNoMap, "Keywords": keywords, "Tags": search_tags, "CategoryId": None,
                      "IsCombine": iscombine, "PageIndex": pageindex, "PageSize": pagesize, "Owner": owner, "Isfavorite": convert_isfavorite}

        get_scenes = configcenter.sceneconfig.get('search_scenes_copy')
        # 查询最低层（第三层）的CategoryID
        if category:
            third_level_category = Task_Search_biz.get_subcategory_id(category)

            if len(third_level_category) > 0:

                third_levels_convert = ','.join(third_level_category)

                # get_scenes=get_scenes%({"CategoryId":third_levels_convert})
                # third_levels_convert = str(third_level_category)[1:-1]
                # par_scenes["CategoryId"] = third_levels_convert

            else:
                third_levels_convert = None
        else:
            third_levels_convert = None
        # get_scenes = configcenter.sceneconfig.get('search_scenes_copy')
        if search_tags is  None:
            search_tags=""
        else:
            search_tags=" AND  tt.[TagId] in (%s)"%search_tags

        if third_levels_convert is  None:
            third_levels_convert=""
        else:
            third_levels_convert=" AND  A.CategoryID in (%s)"%third_levels_convert
        if scene_name is None:
            search_name = ""
        else:
            search_name = " and A.SceneName LIKE N'%{}%'".format(scene_name)
        if keywords is None:
            search_keywords = ""
        else:
            search_keywords = "and A.Keywords LIKE N'%{}%'".format(keywords)
        if description is None:
            description=""
        else:
            description="and A.Description LIKE N'%{}%'".format(description)
        sql=get_scenes%(search_tags,third_levels_convert,search_name,search_keywords,description)
        DB_Session = sessionmaker(bind=db_engine)
        session_sql = DB_Session()
        # m=get_scenes%par_scenes
        # print(get_scenes%par_scenes)
        scenes = session_sql.execute(sql, par_scenes).fetchall()
        session_sql.commit()
        session_sql.close()
        total = len(scenes)
        scenes_list = []
        tags = SearchTagData.get_object_tags_by_from_type("create")
        for scene in scenes:
            scene_tags = []
            is_favorite = 0
            if scene[20] is not None:
                is_favorite = 1
            scene_dict = {"SceneID": scene[1], "SceneName": scene[2], "ProductID": scene[3], "ProductName": scene[4],
                          "Type": scene[5], "Description": scene[6], "CreateUser": scene[7], "GroupID": scene[8],
                          "CreateDate": scene[9], "LastEditDate": scene[10], "MapInfo": scene[11], "UseCount": scene[12],
                          "PublicUpdateStatus": scene[13], "ReferenceCount": scene[14], "UserFullName": scene[15],
                          "IsIncludeSubScene": scene[16], "CategoryId": scene[17], "CategoryName": scene[18],
                          "IsCombine": scene[19], "FavoriteID": scene[20], "IsFavorite": scene[21]}

            # 查询Tags
            for tag in tags:
                if tag[0] == scene[1]:
                    scene_tags.append({"TagId": tag[1], "TagName": tag[2]})
            scene_dict["Tags"] = scene_tags
            scenes_list.append(scene_dict)
        if len(scenes) == 0:
            total_count = 0
        else:
            total_count = scenes[0][0]
        return {"PageSize":pagesize, "PageIndex":pageindex, "TotalCount": total_count, "Data": scenes_list}


    def get_all_step(self):
        get_steps = configcenter.sceneconfig.get('get_all_step')
        step_info = db.session.execute(get_steps).fetchall()
        return step_info

    def add_step_for_scene(self, scenes, steps):
        for s in scenes:
            scene_steps = []
            for step in steps:
                if s['SceneID'] == step[0]:
                    scene_steps.append({"SequenceId": step[1], "StepName": step[2], "StepType": step[3]})
            s["Steps"] = scene_steps
        return scenes

class SearchScenesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SearchScenes):
            return {
                'IsSuccess': True,
                'TotalCount': obj.total,
                'PageIndex':obj.pageindex,
                'PageSize': obj.pagesize,
                'Data': obj.data

            }
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return json.JSONEncoder.default(self, obj)


class SearchExecByScenceID():
    def __init__(self):
        pass

    def getexeclist(self, scenceid):
        par_scence = {"SceneID": scenceid}
        get_execlist = configcenter.sceneconfig.get('get_execlist')
        execs = db_conn.execute(get_execlist, par_scence).fetchall()
        exec_list = []
        for e in execs:
            exec_date = e[6]
            l = self.query_log(exec_date, scenceid)
            exec_info = {
                "ExecID": e[0],
                "SceneID": e[1],
                "ExecName": e[2],
                "Description": e[3],
                "Status": e[4],
                "Inuser": e[5],
                "Indate": e[6].strftime('%Y-%m-%d %H:%M:%S'),
                "LastEditdate": e[7].strftime('%Y-%m-%d %H:%M:%S'),
                "LastEdituser": e[8],
                "ExecEnv": e[9],
                "GroupID": e[10],
                "ExecDisableReason": l
            }
            exec_list.append(exec_info)
        return exec_list

    def query_log(self, exec_date, scene_id):
        request_url = configcenter.dbconfig.get('scene_log_cloud_data')
        request_url_convert = '%s?f_UpdateDate={"$gte": "%s"}&f_SceneId=%s' % (request_url, exec_date, scene_id)
        h = {"Accept": "application/json",
             "Content-Type": "application/json"}
        response = requests.get(url=request_url_convert, headers=h)
        print(response.text)

        logs = json.loads(response.text)
        type_list = []
        for log in logs["rows"]:
            log_detail = list(log["UpdateInfo"].keys())
            for l in log_detail:
                type_list.append(l)
        print(type_list)

        result = None
        if ("Mapping" in type_list and "Self" in type_list) or ("Quote" in type_list and "Self" in type_list):
            result = 3
        elif "Self" in type_list and ("Mapping" not in type_list) and ("Quote" not in type_list):
            result = 1
        elif ("Mapping" in type_list or "Quote" in type_list) and "Self" not in type_list:
            result = 2
        return result


class SearchPermissionByUserID():
    def __init__(self):
        return None

    @row_dict
    def get_permission_by_userid(self, UserID):
        par_scence = {"UserID": UserID}
        get_permission = configcenter.sceneconfig.get('get_isAdmin_mark')
        return db_conn.execute(get_permission, par_scence)


class SearchParamsBySceneTransactionId():
    def __init__(self):
        return None

    @row_dict
    def get_params_by_scenetransactionid(self, SceneTransactionId):
        get_permission = configcenter.sceneconfig.get('get_params_by_scenetransactionid') % str(SceneTransactionId)
        return db_conn.execute(get_permission)

class GetParamRelationShip():
    def __init__(self, scene_id):
        self.scene_id = scene_id

    def get_scene(self):
        par_scene_master = {"SceneID": self.scene_id}
        get_scene_master = configcenter.sceneconfig.get('select_scene_master')
        scene_master = db.session.execute(get_scene_master, par_scene_master).fetchone()
        if not scene_master:
            raise Exception("Scene：'{}' does not exist!".format(self.scene_id))
        else:
            return scene_master

    def check_is_exsits_in_mapping_table(self):
        get_params = configcenter.sceneconfig.get('check_exsits_mapping_table') % int(self.scene_id)
        params = db.session.execute(get_params).fetchone()
        return params

    def get_step_info(self, actual_scene_id):
        par_steps = {"SceneID": actual_scene_id}
        get_steps = configcenter.sceneconfig.get('select_step')
        steps = db.session.execute(get_steps, par_steps).fetchall()
        step_list = []
        for step in steps:
            step_dict = {
                "Id": str(uuid.uuid1()),
                "SequenceID": step[0],
                "Name": step[1],
                "StepType": step[2],
                "DBType": step[3],
                "Database": step[4],
                "ExecScript": step[5],
                "RecoveryID": step[6],
                "APIUrl": step[7],
                "APIHeaders": step[8],
                "APIRequests": step[9],
                "APIMethod": step[10],
                "Protocol": step[11],
                "SceneTransactionID": step[12],
                "Sleep": step[13],
                "SubSceneID": step[14],
                "Expect": step[15],
                "Interval": step[16],
                "Times": step[17],
                "SceneID": step[18]
            }
            step_list.append(step_dict)
        return step_list

    def get_step_output_param(self, actual_scene_id, sequence_id, father_step_id, current_step_path):
        par_params = {"SceneID": actual_scene_id, "SequenceID": sequence_id}
        get_params = configcenter.sceneconfig.get('select_step_params')
        params = db.session.execute(get_params, par_params).fetchall()
        output_param_list = []
        for param in params:
            param_dict = {
                # "Id": str(uuid.uuid1()),
                "ParamType": param[0],
                "Name": param[1],
                "Value": param[2],
                "IsOutPut": param[3],
                "ParamTransactionNumber": param[4],
                "SceneID": param[5],
                "UseStep": param[6],
                "JsonORXmlPath": param[7],
                "OriginalParameterName": param[8],
                "SceneTranactionID": param[9],
                "DefaultValue": param[10],
                "Description": param[11],
                "OutputPath": param[12],
                "OutputObject": param[13],
                "FatherStepId": father_step_id,
                "CurrentStepPath": current_step_path
            }

            # # 如果该Output参数被最外层的场景使用，需要将其与最外层场景重命名后的Output关联起来
            # # 以便于找到和其他Input的参数对应关系
            # param_dict["SceneRenameOutputName"] = []
            # for scene_out_param in scene_output_params:
            #     # 1. scene_out_param["OriginalParameterName"] == param[1]表示output参数被重命名过
            #     # 2. scene_out_param["OriginalParameterName"] == '' and scene_out_param["ParameterName"] == param[1]表示output参数没被重命名过
            #     if (scene_out_param["OriginalParameterName"] == param[1]) or (scene_out_param["OriginalParameterName"] == '' and scene_out_param["ParameterName"] == param[1]):
            #         param_dict["SceneRenameOutputName"].append(scene_out_param["ParameterName"])


            output_param_list.append(param_dict)
        return output_param_list

    # 根据场景ID获取该场景除Output参数外的其他类型参数信息。包括Auto,Manual和Input
    def get_scene_params_but_except_output_param(self, actual_scene_id, father_step_id):
            get_params = configcenter.sceneconfig.get('get_public_params') % actual_scene_id
            params = db.session.execute(get_params).fetchall()
            public_params_list = []

            for p in params:
                # 将使用该参数步骤的SequenceId转换成整数，便于程序使用。
                UseStep = []
                UseStep_temp = eval(p[7]) if isinstance(p[7], str) and p[7] != '' else ""
                if len(UseStep_temp) > 0:
                    for us in UseStep_temp:
                        UseStep.append(int(us))

                # 继续数据库返回的字段值，并转换
                p_dict = {
                    # "Id": str(uuid.uuid1()),
                    "ParamTransactionNumber": p[0],
                    "SceneTranactionID": p[1],
                    "ParamType": p[2],
                    "Name": p[3],
                    "Value": p[4],
                    "Additional": p[5],
                    "SceneID": p[6],
                    "UseStep": UseStep,
                    "JsonORXmlPath": p[8],
                    "IsOutPut": p[9],
                    "OriginalParameterName": p[10],
                    "DefaultValue": p[11],
                    "Description": p[12],
                    "OutputPath": eval(p[13]) if p[13] is not None and p[13] != '' else '',
                    "OutputObject": p[14],
                    "FatherStepId": father_step_id
                }
                public_params_list.append(p_dict)
            return public_params_list

    def get_public_param_detail(self, scene_public_params, param_name):
        for p in scene_public_params:
            if p["Name"] == param_name:
                return p
        return None

    def get_step_params_but_except_output_param(self ,scene_public_params, step_value, father_step_id, current_step_path):
        params_but_except_output_param_list = []
        ACP = AutoCreateParameters_biz()

        # 根据步骤中填充响应的值，解析出参数名字
        script = {"SqlList": step_value}
        stepParams = ACP.getparams(script, 1)

        # 根据参数名字，从数据库公共的参数中获取参数的详细信息
        for p in stepParams:
            param_detail = self.get_public_param_detail(scene_public_params, p)
            if param_detail:
                param_detail["FatherStepId"] = father_step_id
                param_detail["CurrentStepPath"] = current_step_path
                params_but_except_output_param_list.append(param_detail)

        return params_but_except_output_param_list

    def get_input_params_by_scene_id(self, scene_id):
        input_pars = {"SceneID": scene_id}
        get_input = configcenter.sceneconfig.get('select_input_param_by_scene_id')
        input_params_info = db.session.execute(get_input, input_pars).fetchall()
        input_params = []
        for input in input_params_info:
            input_params.append({
                "SceneID": input[0],
                "ParameterName":input[1],
                "ParameterValue":input[2]
            })
        return input_params

    def get_output_params_by_scene_id(self, scene_id):
        output_pars = {"SceneID": scene_id}
        get_output = configcenter.sceneconfig.get('select_output_param_by_scene_id')
        output_params_info = db.session.execute(get_output, output_pars).fetchall()
        output_params = []
        output_stepids = []
        for output in output_params_info:
            output_params.append({
                "SceneID": output[0],
                "SceneTranactionID":output[1],
                "ParameterName":output[2],
                "OriginalParameterName": output[3]
            })
            output_stepids.append(output[1])
        return output_params, list(set(output_stepids))

    def get_input_output_releation_by_scene_id(self, scene_id):
        input_params = self.get_input_params_by_scene_id(scene_id)
        output_params_info = self.get_output_params_by_scene_id(scene_id)
        output_params= output_params_info[0]
        output_stepids = output_params_info[1]
        input_output_relation = []
        for input in input_params:
            for output in output_params:
                if input["ParameterValue"] == output["ParameterName"]:
                    input_output_relation.append(
                        {
                            "SceneTranactionID": output["SceneTranactionID"],
                            "InputName": input["ParameterName"],
                            "OutputName": output["ParameterName"],
                            "OutputOriginalName": output["OriginalParameterName"]
                        }
                    )
        return (input_output_relation,output_stepids)


    def get_step_and_params(self, actual_scene_id, father_step_id, father_input_output_relation, father_output_stepids, output_rename, step_path=None, scene_path=None):
        ACP = AutoCreateParameters_biz()
        step_list = self.get_step_info(actual_scene_id)
        scene_name = ShowScene(actual_scene_id).get_scene(actual_scene_id)[0]

        scene_input_output_relation_info = self.get_input_output_releation_by_scene_id(actual_scene_id)

        scene_input_output_relation = scene_input_output_relation_info[0]
        output_stepids = scene_input_output_relation_info[1]

        print(scene_name)
        scene_public_params = self.get_scene_params_but_except_output_param(actual_scene_id, father_step_id)

        step_final = []
        for step in step_list:
            # 如果step_path和father_step_id全都是None，则证明是最外层的全局场景
            if step_path == None and father_step_id == None:
                current_step_path = str(step['SceneTransactionID'])
                current_scene_path = str(step['SceneID'])
            else:
                current_step_path = str(step_path)  +"-" + str(step['SceneTransactionID'])
                current_scene_path = str(scene_path)+"-" + str(step['SceneID'])
            # 步骤Expect Value中的参数信息
            r_expect = ACP.get_step_expect_value_params(step["Expect"], actual_scene_id, step["SceneTransactionID"], step['SequenceID'], father_step_id, current_step_path)

            step_expect_value_param_list = copy.deepcopy(r_expect)
            # 获取Output参数信息
            r_output_params = self.get_step_output_param(actual_scene_id, step["SequenceID"], father_step_id, current_step_path)
            output_params = copy.deepcopy(r_output_params)

            # 获取步骤除Output外参数信息。包括Auto,Manual和Input
            params_but_except_output_param_list = []
            if step["StepType"] == "DB" or step["StepType"] == "DBCHECK":
                step_value = step["ExecScript"]
                r = self.get_step_params_but_except_output_param(scene_public_params,step_value, father_step_id, current_step_path)
                params_but_except_output_param_list = copy.deepcopy(r)
            elif step["StepType"] == "API" or step["StepType"] == "APICHECK":
                step_value = step["APIUrl"] + step["APIHeaders"] + step["APIRequests"]
                r = self.get_step_params_but_except_output_param(scene_public_params,step_value, father_step_id, current_step_path)
                params_but_except_output_param_list = copy.deepcopy(r)
            else:
                step_dict = self.get_step_and_params(step["SubSceneID"], step["SceneTransactionID"], scene_input_output_relation, output_stepids, output_rename, current_step_path,current_scene_path)

            if step["StepType"] != "SCENE":
                # 将Param的信息去重
                param_list = self.step_param_distinct(step_expect_value_param_list, output_params,
                                                      params_but_except_output_param_list, scene_input_output_relation, output_stepids, father_input_output_relation, father_output_stepids,current_step_path,current_scene_path,output_rename)

                # # 如果参数列表中包含Input和Output，并且他们之间存在联系的话，则回填ParamList中的RelationShip字段
                # self.get_param_input_output_relationship_detail(scene_input_output_relation, father_input_output_relation, param_list)
                step_dict = copy.deepcopy(step)
                step_dict["children"] = param_list
                step_dict["Type"] = "Step"

            step_final.append(step_dict)
        return {"SceneId": actual_scene_id, "Id": str(uuid.uuid1()), "Name": scene_name,"children": step_final}

    def get_param_input_output_relationship_detail(self, scene_input_output_relation, father_input_output_relation, param_list):
        print(scene_input_output_relation)
        print(father_input_output_relation)
        print(param_list)
        print(1)


    def step_param_distinct(self, step_expect_value_param_list, output_params,
                                                      params_but_except_output_param_list, scene_input_output_relation_info, output_stepids, father_input_output_relation, father_output_stepids,current_step_path,current_scene_path,output_rename):
        step_param_distinct_result = []
        # 步骤期望值的参数去重
        for expect_param in step_expect_value_param_list:
            contain = False
            for p_d in step_param_distinct_result:
                if p_d["ParamTransactionNumber"] == expect_param["ParamTransactionNumber"] and p_d["CurrentStepPath"] == expect_param["CurrentStepPath"]:
                    contain = True
            if contain == False:
                expect_param["Id"] =str(uuid.uuid1())
                expect_param["SceneInputOutputRelation"] = scene_input_output_relation_info
                expect_param["SceneOutputStepids"] = output_stepids
                expect_param["FatherInputOutputRelation"] = father_input_output_relation
                expect_param["FatherOutputStepids"] = father_output_stepids
                expect_param["StepPath"] = current_step_path
                expect_param["CurrentScenePath"] = current_scene_path
                expect_param["Type"] = "Param"
                step_param_distinct_result.append(expect_param)

        # Output参数去重
        for out_param in output_params:
            contain = False
            for p_d in step_param_distinct_result:
                if p_d["ParamTransactionNumber"] == out_param["ParamTransactionNumber"] and p_d["CurrentStepPath"] == out_param["CurrentStepPath"]:
                    contain = True
            if contain == False:
                # 如果output参数是重命名过的，则需要将其参数名字覆盖成重命名后的
                for out in output_rename:
                    if out['OriginParamId'] == out_param['ParamTransactionNumber'] and out['GlobalStepId'] == out_param['FatherStepId']:
                        out_param['Name'] = out['RenameParamName']
                        out_param['OriginalParameterName'] = out['OriginParamName']
                out_param["Id"] = str(uuid.uuid1())
                out_param["SceneInputOutputRelation"] = scene_input_output_relation_info
                out_param["SceneOutputStepids"] = output_stepids
                out_param["FatherInputOutputRelation"] = father_input_output_relation
                out_param["FatherOutputStepids"] = father_output_stepids
                out_param["StepPath"] = current_step_path
                out_param["CurrentScenePath"] = current_scene_path
                out_param["Type"] = "Param"
                step_param_distinct_result.append(out_param)

        # 除Output外其他公共参数去重
        for public_param in params_but_except_output_param_list:
            contain = False
            for p_d in step_param_distinct_result:
                if p_d["ParamTransactionNumber"] == public_param["ParamTransactionNumber"] and p_d["CurrentStepPath"] == public_param["CurrentStepPath"]:
                    contain = True
            if contain == False:
                public_param["Id"] = str(uuid.uuid1())
                public_param["SceneInputOutputRelation"] = scene_input_output_relation_info
                public_param["SceneOutputStepids"] = output_stepids
                public_param["FatherInputOutputRelation"] = father_input_output_relation
                public_param["FatherOutputStepids"] = father_output_stepids
                public_param["StepPath"] = current_step_path
                public_param["CurrentScenePath"] = current_scene_path
                public_param["Type"] = "Param"
                step_param_distinct_result.append(public_param)
        return step_param_distinct_result

    # # 根据场景ID查询该场景的所有output参数
    # # 为的是查询最外层场景的output参数，以便于重名时，能够找到重名后的output参数和input的对应关系
    # def get_output_params_by_scene_id(self):
    #     par_steps = {"SceneID": self.scene_id}
    #     get_steps = configcenter.sceneconfig.get('select_output_param_by_scene_id')
    #     output_params = db.session.execute(get_steps, par_steps).fetchall()
    #     output_param_list = []
    #     for p in output_params:
    #         output_param_list.append({
    #             "SceneID": p[0],
    #             "SceneTranactionID": p[1],
    #             "Type": p[2],
    #             "ParameterName": p[3],
    #             "OriginalParameterName": p[4],
    #         })
    #     return output_param_list




    def get_params_releation(self):
        # 获取场景基本信息
        scene = self.get_scene()

        # 获取场景的类型，因为涉及到clone等操作，所以需要根据Type做特殊逻辑获取信息
        scene_type = scene[4]

        # 默认真实场景ID为用户传的。
        # 如果该场景是私有场景，并且该场景是从公有拉过来的，则真实场景ID为公共的场景ID
        actual_scene_id = self.scene_id
        if scene_type=='Private':
            is_exsits_in_mapping_table = self.check_is_exsits_in_mapping_table()
            if is_exsits_in_mapping_table:
                actual_scene_id = is_exsits_in_mapping_table[0]

        # 同一个场景被添加多次，会对Output参数进行重命名。
        # Output参数全局唯一。
        # 本方法返回重命名后的名字与原始名字的对应关系
        output_rename = self.get_scene_output_param_rename()

        # 根据场景ID获取步骤信息以及参数信息
        r = self.get_step_and_params(actual_scene_id, None, [], [], output_rename)

        # 重写Input的Type和Value。
        # 因为可能会存在以下情况：子场景的Input是Manual的类型，并且Value是空的。但是该父场景将其与某个Output参数关联了。
        # 最终的结果是子场景类型是Manual，但是父场景中该参数的类型是Input。最终导致同一个参数父子类型矛盾的情况。
        # 我们认为父场景中的参数类型应该为最新的，所以以下方法实现了将Manual参数的类型和Value以父场景的类型重写
        self.rewrite_subscene_manual_param_type_value_by_father(r, r)

        # 获取Input和Output之间的参数关系
        self.get_releation_finally(r,r)

        return r

    def get_scene_output_param_rename(self):
        get_sql = configcenter.sceneconfig.get('get_scene_output_param_rename')
        param = {"scene_id": self.scene_id}
        result = db.session.execute(get_sql, param).fetchall()
        final_result = []
        for r in result:
            final_result.append({
                "RenameParamName": r[3],
                "OriginParamName": r[4],
                "GlobalStepId": r[5],
                "OriginParamId": r[6]
            })
        return final_result


    def rewrite_subscene_manual_param_type_value_by_father(self, scene, sub_scene):
        # 最外层的场景，也就是用户需要查询的场景。
        # 这个场景信息永远也不能变
        all_scene = copy.deepcopy(scene)
        # sub_scene为当前处理的场景。
        # 第一次sub_scene为最外层的场景。
        # 递归调用时，sub_scene为步骤信息
        for child in sub_scene["children"]:
            # 如果是普通的Step，则child["Children"]则为参数信息
            if len(child["children"])>=1 and "ParamTransactionNumber" in child["children"][0]:
                for param in child["children"]:
                    if param["ParamType"] == "Manual":
                        self.rewrite_subscene_manual_param_type_value(param)
            else:
                self.rewrite_subscene_manual_param_type_value_by_father(all_scene, child)



    def rewrite_subscene_manual_param_type_value(self, manual_param):
        # 1. 该参数所在的步骤，或者其步骤的爸爸，步骤的爷爷等等，都有可能会修改该参数，将其从Manual修改为Input
        # 2. 参数的类型为Input
        # 符合以上条件，则认为子场景中的Manual参数被父场景重写成了Input参数了
        scene_path_convert =",".join(manual_param['CurrentScenePath'].split("-"))
        get_sql = configcenter.sceneconfig.get('select_father_rewite_manual_param') % (scene_path_convert, manual_param['Name'])
        output_name = db.session.execute(get_sql).fetchone()
        if output_name == None:
            return
        manual_param['ParamType'] = "Input"
        manual_param['Value'] = output_name[0]

    def get_input_param_by_value(self, output_param, scene, param_list):
        for child in scene["children"]:
            if len(child["children"])>=1 and "ParamTransactionNumber" in child["children"][0]:
                # 如果是普通的Step，则child["Children"]则为参数信息
                for param in child["children"]:
                    # 1. 参数类型是Input
                    # 2. 它的Value与output的参数名相等
                    # 3. 他们step_path最外层的的值是一样的。即他们在全局的场景中，是属于同一个sub scene的
                    input_step_path_global = param["StepPath"].split("-")[0]
                    output_step_path_global = output_param["StepPath"].split("-")[0]

                    # 如果len(output_param["StepPath"].split("-")) > 1，说明它的Path是类似这种1333-1231-32432，即他所在的步骤是嵌套三层以及以上的。
                    # 这种情况需要判断他们的全局父场景是同一个
                    if len(output_param["StepPath"].split("-")) > 2 or len(param["StepPath"].split("-")) > 2:
                        if param["ParamType"] == "Input" and param["Value"] == output_param[
                            "Name"] and input_step_path_global == output_step_path_global:
                            param_list.append(param)
                    else:
                        if param["ParamType"] == "Input":
                            # 嵌套场景内部，就直接用Name本身
                            if param["FatherStepId"] == output_param["FatherStepId"] and (param["Value"] == output_param["Name"] or param["Value"] == output_param["OriginalParameterName"]):
                                param_list.append(param)
                            elif param["SceneID"] != output_param["SceneID"] and param["Value"] == output_param["Name"]:
                                param_list.append(param)
            else:
                param_list = self.get_input_param_by_value(output_param, child, param_list)
                other_childs = self.get_same_level_other_step(child, scene)
                if other_childs:
                    param_list = self.get_input_param_by_value(output_param, other_childs, param_list)
        return param_list

    def get_same_level_other_step(self, child, father):
        father_copy = copy.deepcopy(father)
        for index, Element in enumerate(father['children']):
            if Element["Id"] == child["Id"]:
                father_copy["children"].pop(index)
        return father_copy

    def get_output_param_by_name_detail(self, child, input_param, param_list):
        for param in child["children"]:
            # 1. 参数类型是Output
            # 2. 它的Name与Input的参数值相等
            # 3. 他们step_path最外层的的值是一样的。即他们在全局的场景中，是属于同一个sub scene的
            output_param_step_path = param["StepPath"].split("-")[0]
            input_param_step_path = input_param["StepPath"].split("-")[0]

            # 如果len(input_param["StepPath"].split("-")) > 1，说明它的Path是类似这种1333-1231，即input参数所在的步骤是嵌套的子场景。
            # 这种情况需要判断他们的父场景是同一个
            if len(input_param["StepPath"].split("-")) > 2 or len(param["StepPath"].split("-")) > 2:
                if param["ParamType"] == "Output" and param["Name"] == input_param["Value"] and output_param_step_path == input_param_step_path:
                    param_list.append(param)
            # 否则它不是嵌套的，只是单纯的场景，那么只需要判断类型和参数名是否为Input值即可
            else:
                if param["ParamType"] == "Output":
                    # 嵌套子场景内部的参数关系
                    if param["FatherStepId"] == input_param["FatherStepId"] and (param["OriginalParameterName"] == input_param["Value"] or param["Name"] == input_param["Value"]):
                        param_list.append(param)
                    elif param["SceneID"] != input_param["SceneID"] and param["Name"] == input_param["Value"]:
                        param_list.append(param)

    def get_output_param_by_name(self, input_param, scene, param_list):
        if input_param["Name"] == "Step4-Combined-Param1":
            print(1)

        for child in scene["children"]:
            # 如果是普通的Step，则child["Children"]则为参数信息
            if len(child["children"])>=1 and "ParamTransactionNumber" in child["children"][0]:
                self.get_output_param_by_name_detail(child, input_param, param_list)
            else:
                # 如果Step是一个场景
                # 1. 需要找该Step中符合要求的参数
                # 2. 需要找与该Step并行的其他步骤符合要求的参数
                param_list = self.get_output_param_by_name(input_param, child, param_list)
                other_childs = self.get_same_level_other_step(child, scene)
                if other_childs:
                    param_list = self.get_output_param_by_name(input_param, other_childs, param_list)
                # for other_child in other_childs:
                #     param_list += get_output_param_by_name(input_param, other_child, param_list)
        return param_list

    def get_releation_finally(self, scene, sub_scene):
        # 最外层的场景，也就是用户需要查询的场景。
        # 这个场景信息永远也不能变
        all_scene = copy.deepcopy(scene)

        # sub_scene为当前处理的场景。
        # 第一次sub_scene为最外层的场景。
        # 递归调用时，sub_scene为步骤信息
        for child in sub_scene["children"]:
            # 如果是普通的Step，则child["Children"]则为参数信息
            if len(child["children"])>=1 and "ParamTransactionNumber" in child["children"][0]:
                for param in child["children"]:
                    # 如果参数类型是Output，并且该参数所在的步骤是与其他步骤的Input参数是存在关系的
                    # param["SceneOutputStepids"]表示的是步骤Id,这些步骤的Output参数值会作为其他Input参数的输入
                    # 参数关联关系：整个场景中类型是Input并且Value是Output参数名
                    if param["ParamType"] == "Output" and param["SceneTranactionID"] in param["SceneOutputStepids"]:
                        input_params = self.get_input_param_by_value(param, all_scene, [])
                        param["ReleationShip"] = self.distinct_params(input_params)

                    if param["ParamType"] == "Input":
                        # 获取与Input参数关联的Output参数
                        output_params = self.get_output_param_by_name(param, all_scene, [])
                        param["ReleationShip"] = self.distinct_params(output_params)
            else:
                self.get_releation_finally(all_scene, child)

    def distinct_params(self, params):
        distinct_params = []
        distinct_params_id = []
        distinct_params_step_path = []
        for p in params:
            # 因为参数id是全局唯一的，通过参数名去重即可
            if p["Id"] in distinct_params_id and p["CurrentStepPath"] in distinct_params_step_path:
                continue
            distinct_params.append(p)
            distinct_params_id.append(p["Id"])
            distinct_params_step_path.append(p["CurrentStepPath"])
        return distinct_params
