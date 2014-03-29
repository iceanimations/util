from site import addsitedir as asd
asd(r"r:/Pipe_Repo/Users/Hussain/utilities/TACTIC")
from datetime import datetime
_s = None

def set_server(server):
    global _s
    _s = server

try:
    from auth import user as USER
    user = USER.get_user()
    set_server(USER.get_server())
    def get_all_task(user = USER.get_user()):
        return [task.get("__search_key__", '') for proj in get_all_projects()
                for task in all_tasks(proj, user = user)]
        
    def get_all_project_user_sobjects(user = USER.get_user()):
        proj_dict = {}
        for proj in get_all_projects():
            key = proj['code']
            sobj = get_user_related_sobjects(proj, user)
            if sobj:
                proj_dict[key] = sobj
        return proj_dict

    def get_user_related_sobjects(proj, user = USER.get_user()):
        '''
        @return: sobject_search_key
        '''
        return map_tasks_to_sobjects([task
                                      for task in all_tasks(proj, user = user)
                                      if str(task["status"]).lower() != "done"]).keys()

    def sobject_to_user_task(sobj, user = USER.get_user()):

        proj = _s.get_project()
        start = sobj.find('project=')
        project = sobj[start:start + sobj[start:].find('&')]
        _s.set_project(project.replace("project=", ""))
        tasks = _s.get_all_children(sobj, "sthpw/task",
                                    filters = [("assigned", str(user))])
        _s.set_project(proj)
        return [task.get('__search_key__') for task in tasks]

    def user_project_task(proj, user = USER.get_user()):
        return {
            "proj": {
                "sobject_uniq_key": {
                    "task_uniq_key": {
                        "process": "name of process", 
                        "context": {
                            "name of context": {
                                "snapshots": []
                            }
                        }
                    }
                }
            }
        }
        main_dict = {}
        tasks = all_user_tasks = all_task(proj, user = user)
        sobj_tasks = map_tasks_to_sobjects(tasks)
        sobj_snaps = map_sobject_to_snap(sobj_tasks.keys())

        proj = {}

        for sobj, tasks in sobj_tasks.iteritems():
            sobj_dict = {}
            proj[sobj] = sobj_dict
            for task in tasks:
                task_dict = {}
                sobj_dict[task["__search_key__"]] = task_dict
                task_dict["process"] = task["process"]
                context_dict = {}
                task_dict["context"] = context_dict

                snaps = sobj_snaps[sobj]

                for snap in snaps:

                    if snap["process"].lower() == task["process"].lower():

                        if not context_dict.has_key(snap["context"]):
                            context_dict[snap["context"]] = []

                        context_dict[snap["context"]].append(snap['__search_key__'])

                if not context_dict:
                    context_dict[task["process"]] = []

except:
    user = None
    from client.tactic_client_lib import TacticServerStub
    set_server(TacticServerStub.get())
    
def get_server(): return _s
def get_all_projects():
    project = _s.get_project()
    _s.set_project("sthpw")
    projects = _s.query("sthpw/project")
    _s.set_project(project)
    map(projects.pop, [ind for ind in reversed(range(len(projects)))
                       if (# filter 'admin' and 'sthpw'
                               projects[ind]["code"] in ["admin", "sthpw"]
                               # filter all template project
                               or projects[ind]["is_template"]
                               # filter all Sample Projects
                               or projects[ind]['category'] == 'Sample Projects')
                       # make exception for the vfx project
                       and not projects[ind]['code'] == 'vfx'])
    return projects

def map_sobject_to_snap(sobjs):
    sobj_map = {}
    for sobj in sobjs:
        try:
            sobj_map[sobj] = get_snapshot_from_sobject(sobj)
        except:
            sobj_map[sobj] = []
    return sobj_map

# def get_all_projects(clean = False):
#     projects = all_projects()
#     if clean:
#         pop = ['s_status', 'db_resource', 'id', 'sobject_mapping_cls', 'palette',
#                '__search_key__', 'dir_naming_cls', 'status', 'code_naming_cls',
#                'file_naming_cls', 'reg_hours', 'last_db_update', 'pipeline',
#                'database', 'last_version_update', 'is_template', 'snapshot',
#                'node_naming_cls', 'initials']
#         [map(proj.pop, pop) for proj in projects]
#         for proj in projects:
#             proj["start_date"] = all
#     return projects

def project_start_date(project):
    pass

def team_involved(project):
    pass

def get_sobject_from_task(task):
    '''
    @task: task dict or task string
    '''
    if not isinstance(task, dict):
        task = _s.get_by_search_key(task)
        
    search_type = task["search_type"]
    if not search_type: return None
    return _s.build_search_key(search_type[:search_type.find("?")
                                               if not -1
                                               else len(search_type)],
                                   task["search_code"],
                                   project_code = task.get("project_code"))

def map_tasks_to_sobjects(tasks):

    '''
    @return: {sobject_search_key: [tasks_database_dicts,...]}
    '''
    s_tasks = {}
    for task in tasks:
        
        sobj = get_sobject_from_task(task)
        if not sobj: continue
        try:
            s_tasks[sobj].append(task)
        except:
            s_tasks[sobj] = [task]
    return s_tasks

# def filter_user_tasks(user, tasks):
#     '''
#     Extract the tasks that belong ot `user' from a list `tasks'.
#     @user: the user who's tasks are to be extracted
#     @tasks: list of task
#     '''
#     return filter(lambda task: task["assigned"] == user, tasks)

def project_code_to_title(proj_code):
    '''
    @return: the project title given it's code. None if the project code is not listed.
    '''
    return _s.query("sthpw/project", [("code", proj_code)], single = True).get("title")

def project_from_tasks(tasks):
    '''
    @tasks: task database column dict
    '''
    return set([task["project_code"] for task in tasks])

def get_project_from_task(task):
    return _s.get_by_search_key(task)['project_code']

def get_snapshot_from_sobject(sobj):
    proj = _s.get_project()
    start = sobj.find('project=')
    project = sobj[start:start + sobj[start:].find('&')]
    _s.set_project(project.replace("project=", ""))
    snapshots =  _s.get_all_children(sobj, "sthpw/snapshot")
    for index in reversed(range(len(snapshots))):
        if 'icon' in [snapshots[index].get(key).lower()
                      for key in ['process', 'context']]:
            snapshots.pop(index)
    _s.set_project(proj)
    return snapshots

def get_sobject_from_snap(snap):
    '''
    @snap: snap dict or snap string
    '''
    if not isinstance(snap, dict):
        snap = _s.get_by_search_key(snap)
        
    search_type = snap["search_type"]
    if not search_type: return None
    return _s.build_search_key(search_type[:search_type.find("?")
                                               if not -1
                                               else len(search_type)],
                                   snap["search_code"],
                               project_code = snap.get("project_code"))

def get_sobject_name(sobj):
    sobj_dict =  _s.get_by_search_key(sobj)
    return sobj_dict.get("title",
                         sobj_dict.get("name",
                                       sobj_dict.get("code")))

# def add_snapshot_to_task(sobjs_to_tasks):

#     '''
#     @sobj_to_task: dict of structure -> {sobj: [task,..]}
#                    where sboj is the search_key
#     @return: {sobj: {task:[snapshot,..]}}
#     '''

#     snapshoted = {}

#     for sobj, tasks in sobjs_to_tasks.iteritems():
#         print sobj
#         try:
#             sobj_snaps = get_snapshot_from_sobject(sobj)
#         except:
#             continue
#         task_snap = {}
#         for task in tasks:
#             process = task["process"]
#             snaps = []
#             task_snap[task["__search_key__"]] = {"task": task, "snaps": snaps}
            
#             for snap in sobj_snaps:
#                 if snap["process"] == process:
#                     snaps.append(snap)
                
#         snapshoted[sobj] = task_snap
#     return snapshoted
def all_tasks(project, process = None, user = None, clean = False):

    '''
    @project: takes in the project code
    @process: take in a single process whose tasks shall be returned. None implies all.
    '''
    
    proj = current_project()
    _s.set_project(project["code"])
    
    filters = [("process", process)] if process else []
    filters.append(("project_code", project["code"]))

    if user is True:
        filters.append(("assigned", _s.get_login()))
    elif user:
        filters.append(("assigned", user))
    else: pass
    
    tasks = _s.query("sthpw/task", filters = filters)
    if clean:
        pop = [drat for drat in range(len(tasks))
               if "?project=%s" %project not in str(tasks[drat]["search_type"])]
        map(tasks.pop, reversed(pop))
    _s.set_project(proj)
    return tasks

def all_assets(project):
    proj = current_project()
    _s.set_project(project)
    result =  _s.query('vfx/asset'# , filters = [("project_code", project)]
    )
    _s.set_project(proj)
    return result

def get_project_snapshots(proj):
    return _s.query('sthpw/snapshot', filters = [('project_code', proj)])

def task_details(project):
    pass

def production_dept(project):
    pass

def current_project(): return _s.get_project()
def all_task_processes(project, tasks = None):
    processes = set([task["process"] for task in (all_tasks(project)
                                                  if not tasks else tasks)])
    return list(processes)

def all_process_tasks(project, process = None, tasks = None):
    processes = set([task["process"] for task in (all_tasks(project)
                                                  if not tasks else tasks)])
    tasks = {}
    for proc in list(processes):
        # get rid of this call to all_tasks
        t = all_tasks(project, proc)
        tasks[proc] = t

    return tasks[process] if process else tasks

def date_str_to_datetime(string, format = "%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(string.split(".")[0], format)

def get_project_title(proj_code):
    result = _s.query("sthpw/project", filters = [("code", proj_code)])
    if result:
        return result[0]["title"]
    else:
        return None

def get_task_process(task):
    return _s.get_by_search_key(task)["process"]

def get_contexts_from_task(task):

    sobj = get_sobject_from_task(task)
    task_sobj = _s.get_by_search_key(task)
    contexts = set([item['context']
                    for item in get_snapshot_from_sobject(sobj)
                    if item["process"].lower() == task_sobj["process"]])

    return list(contexts) if contexts else [task_sobj['process']]

def get_sobject_description(sobj):
    return "description for sobject: " +  sobj

def get_snapshots(context, task):
    '''
    @context: context of the task
    @task: search_key of the task
    @return: {snapshot_search_key: {'filename': file basename at sandbox (str),
                                    'version': snapshot version (int),
                                    'latest': is this version latest (bool),
                                    'description': description of the snapshot}
    '''
    import os.path as op
    sobj = get_sobject_from_task(task)

    snapshots = get_snapshot_from_sobject(sobj)

    snapshots = [snap for snap in snapshots
                 if snap["process"] == _s.get_by_search_key(task)["process"]]
    snapshot_dict = {}
    
    for snap in snapshots:
        snapshot_dict[snap["__search_key__"]] = {"filename": op.basename(
            filename_from_snap(snap)),
                                                 "version": snap["version"],
                                                 "latest": snap["is_latest"],
                                                 "description": snap[
                                                     'description'],
                                                 'timestamp': snap['timestamp']
        }
    return snapshot_dict

def filename_from_snap(snap, mode = 'sandbox'):
    '''
    @snap: db dict
    '''
    pretty_print(snap)
    return _s.get_all_paths_from_snapshot(snap['__search_key__'],
                                          mode = mode)[0]
    
def pretty_print(obj, ind = 2):
    import json
    print json.dumps(obj, indent = ind)

