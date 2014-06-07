'''
This module can work both on server and client side.
'''

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
        
        user = USER.get_user()
        return [task.get("__search_key__", '') for proj in get_all_projects()
                for task in all_tasks(proj, user = user)]

    def get_all_project_user_sobjects(user = USER.get_user()):
        '''
        
        '''
        user = USER.get_user()
        proj_dict = {}
        for proj in get_all_projects():
            key = proj['code']
            sobj = get_user_related_sobjects(proj, user)
            if sobj:
                proj_dict[key] = sobj
                
        return proj_dict

    def get_user_related_sobjects(proj, user = USER.get_user()):
        '''
        @return: sobject_search_key list
        '''
        user = USER.get_user()
        return map_tasks_to_sobjects(
            [task
             for task in all_tasks(proj, user = user)
             if str(task["status"]).lower() != "done"]).keys()

    def sobject_to_user_task(sobj, user = USER.get_user()):
        user = USER.get_user()
        proj = _s.get_project()
        start = sobj.find('project=')
        project = sobj[start:start + sobj[start:].find('&')]
        set_project(project = project.replace("project=", ""))
        tasks = _s.get_all_children(sobj, "sthpw/task",
                                    filters = [("assigned", str(user))])
        set_project(project = proj)
        return [task.get('__search_key__') for task in tasks]


except:
    user = None
    from client.tactic_client_lib import TacticServerStub
    set_server(TacticServerStub.get())

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

def current_project():
    return _s.get_project()

def date_str_to_datetime(string, format = "%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(string.split(".")[0], format)

def filename_from_snap(snap, mode = 'sandbox'):
    '''
    @snap: db dict
    '''
    set_project(project = snap['project_code'])
    return _s.get_all_paths_from_snapshot(snap['__search_key__'],
                                          mode = mode)[0]

# def filter_user_tasks(user, tasks):
#     '''
#     Extract the tasks that belong ot `user' from a list `tasks'.
#     @user: the user who's tasks are to be extracted
#     @tasks: list of task
#     '''
#     return filter(lambda task: task["assigned"] == user, tasks)

def get_project_from_search_key(s_key):
    
    if 'project' in s_key:
        prj_tag = 'project='
        return s_key[s_key.find(prj_tag) + len(prj_tag)
                       :s_key.find('&')]
    else:
        return 'sthpw'
    
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
def get_server():
    return _s

def get_all_projects():
    project = _s.get_project()
    set_project(project = "sthpw")
    projects = _s.query("sthpw/project")
    set_project(project = project)
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

def get_project_from_task(task):
    
    return _s.get_by_search_key(task)['project_code']

def get_snapshot_from_sobject(sobj):
    
    proj = _s.get_project()
    set_project(search_key = sobj)
    snapshots =  _s.get_all_children(sobj, "sthpw/snapshot")
    
    for index in reversed(range(len(snapshots))):
        
        if 'icon' in [snapshots[index].get(key).lower()
                      for key in ['process', 'context']]:
            snapshots.pop(index)
            
    set_project(project=proj)
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
    title =  sobj_dict.get("title",
                           sobj_dict.get("name",
                                         'No title'))
    # because of the changes in the new release
    return title + '(%s)'%sobj_dict.get('code')

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
def get_tasks(project, process = None, user = None, clean = False):

    '''
    @project: takes in the project code
    @process: take in a single process whose tasks shall be returned. None implies all.
    '''
    
    proj = current_project()
    set_project(project = project["code"])
    filters = [("process", process)] if process else []
    filters.append(("project_code", project["code"]))

    if user is True:
        filters.append(("assigned", _s.get_login()))
    elif user:
        filters.append(("assigned", user))
    else:
        pass
    
    tasks = []
    print "project:", _s.get_project()
    try:
        tasks = _s.query("sthpw/task", filters = filters)
        if clean:
            pop = [drat for drat in range(len(tasks))
                   if "?project=%s" %project not in
                   str(tasks[drat]["search_type"])]
            map(tasks.pop, reversed(pop))
    except:
        print 'ERROR!!!!'
        pass
    
    pretty_print(tasks)
    set_project(project = proj)
    
    return tasks

def get_episodes(project):
    '''
    @project: project search key
    @return: list of all episode db dicts
    '''
    proj = current_project()
    set_project(project = project)
    result = _s.query('vfx/episode')
    set_project(project=proj)
    return result

def get_sequences(project, episode = None):
    '''
    @project: project search key
    @episode: episode search key
    @return: list of filtered sequence db dicts
    '''
    proj = current_project()
    set_project(project = project)
    result = _s.query('vfx/sequence',
                      filters = [('episode_code', get_search_key_code(episode))]
                      if episode else [])
    set_project(project = proj)
    return result

def get_shots(project, sequence = None):
    '''
    @project: project search key
    @sequence: sequence search key
    @return: list of filtered shot db dicts
    '''
    proj = current_project()
    set_project(project = project)
    result = _s.query('vfx/shot',
                      filters = [('sequence_code', sequence)]
                      if sequence else [])
    set_project(project = proj)
    return result

def get_assets(project):
    proj = current_project()
    set_project(project = project)
    result =  _s.query('vfx/asset'# , filters = [("project_code", project)]
    )
    set_project(project = proj)
    return result

def get_assets_in_shot(project, shot):
    '''
    @project: project search key
    @shot: shot search key
    @return: list of asset db dicts
    '''
    proj = current_project()
    set_project(project = project)
    result = map(_s.get_by_search_key,
                 map(lambda code: _s.build_search_key('vfx/asset',
                                                      code['asset_code'],
                                                      project_code = project),
                     _s.query('vfx/asset_in_shot',
                              filters = [('shot_code',
                                          _s.get_by_search_key(shot)['code'])],
                             columns = ['asset_code'])))
    set_project(proj)
    return result
    
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

def get_project_snapshots(proj):
    return _s.query('sthpw/snapshot', filters = [('project_code', proj)])

def get_search_key_code(search_key):
    '''
    @search_key: sobject search_key
    @return: code
    '''
    return _s.split_search_key(search_key)[0]
    
def get_all_users():
    return _s.query("sthpw/login")

def map_sobject_to_snap(sobjs):
    sobj_map = {}
    for sobj in sobjs:
        try:
            sobj_map[sobj] = get_snapshot_from_sobject(sobj)
        except:
            sobj_map[sobj] = []
    return sobj_map

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

def production_dept(project):
    pass

def project_start_date(project):
    pass

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

def pretty_print(obj, ind = 2):
    import json
    print json.dumps(obj, indent = ind)

def set_project(project = None, search_key = None):

    server = _s
    
    if project and project != server.get_project():
        server.set_project(project)
    elif search_key:
        server.set_project(get_project_from_search_key(search_key))

def task_details(project):
    pass

def team_involved(project):
    pass

all_assets = get_assets
all_tasks = get_tasks
