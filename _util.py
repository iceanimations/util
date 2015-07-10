'''
This module can work both on server and client side.
'''

from site import addsitedir as asd
asd(r"r:/Pipe_Repo/Users/Hussain/utilities/TACTIC")
from datetime import datetime
import json, os
op = os.path

_s = None

def set_server(server):
    global _s
    _s = server

try:
    from auth import user as USER
    user = USER.get_user()
    set_server(USER.get_server())

    def get_all_task(user = USER.get_user()):
        '''
        :return: list of task search keys
        '''
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


except Exception as e:
    user = None
    #from tactic_client_lib import TacticServerStub
    #set_server(TacticServerStub.get(setup=False))
    from auth import user as USER
    set_server(USER.TacticServer(setup=False))

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

def cacheable(obj):
    '''
    :return: True if the object belongs to a TACTIC referenced file, else False
    :obj: Maya object
    '''
    import imaya as mi
    reload(mi)
    print mi.pc.PyNode(obj).referenceFile()
    print get_references().keys()
    return True if mi.pc.PyNode(obj).referenceFile() in get_references().keys() else False

def date_str_to_datetime(string, format = "%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(string.split(".")[0], format)

def get_filename_from_snap(snap, mode = 'sandbox'):
    '''
    @snap: db dict
    '''
    set_project(project = snap['project_code'])
    path = _s.get_all_paths_from_snapshot(snap['__search_key__'],
                                          mode = mode)[0]
    return path if path else None

filename_from_snap = get_filename_from_snap
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
    :task: task dict or task string
    :return: search_key of the associated sobject
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
    '''
    :sobj: search key of the sobj
    '''

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
    #print "project:", _s.get_project()
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

def get_shots(project, sequence = None, episode=None):
    '''
    @project: project search key
    @sequence: sequence search key
    @return: list of filtered shot db dicts
    '''
    proj = current_project()
    set_project(project = project)
    result = _s.query('vfx/shot',
                      filters = [('sequence_code', get_search_key_code(sequence)
                                  if sequence else
                                  ([seq.get('code')
                                    for seq in _s.query(
                                            'vfx/sequence', filters = [
                                                ('episode_code',
                                                 get_search_key_code(episode))])]
                                   if episode else ''))]
                      if (sequence or episode) else [])
    set_project(project = proj)
    return result


def get_assets(project, add_icons=False):
    ''' get all `assets` in a vfx project 
    @param project: code of the project being queried
    @type project: string
    @param add_icons: set this to true if you want icon paths as a list in the
    dictionary accessible by the key 'icon'. Use this with caution because it
    might slow the call down if there are a lot of assets.

    @returns: sobject dictionary
    @rtype: dict
    '''
    proj = current_project()
    set_project(project = project)
    result =  _s.query('vfx/asset')
    if add_icons:
        for asset in result:
            asset['icon'] = get_icon(asset)
    set_project(project = proj)
    return result


def get_icon(obj, mode='client_repo', file_type='icon'):
    ''' Get an icon for file, path, snapshot or sobject

    :param obj: filepath or skey or sobject dict of file, snapshot or sobject
    for which icon is required
    :type obj: dict or str or unicode
    '''
    try:
        if isinstance(obj, dict):
            obj = obj.get('__search_key__')
        stype, code = _s.split_search_key(obj)
        if obj.startswith('sthpw/file'):
            return get_file_icon(obj, mode=mode, file_type=file_type)
        elif obj.startswith('sthpw/snapshot'):
            return get_snapshot_icon(obj, mode=mode, file_type=file_type)
        elif obj.startswith('sthpw/task'):
            return get_task_icon(obj, mode=mode, file_type=file_type)
        return get_sobject_icon(obj, mode=mode, file_type=file_type)
    except (AssertionError, ValueError, KeyError, AttributeError):
        return get_path_icon(obj, mode=mode, file_type=file_type)


def get_sobject_icon(sobject_skey, mode='client_repo', file_type='icon'):
    ''' get the icon path of the given sobject

    @param sobject: sobject searchkey for which the icon is required
    @keyparam mode: the type of the path required, leave it to default
    client_repo for production utilities
    @keyparam file_type: the type of file required, only 'main', 'icon' and
    'web' are relevant
    '''
    process = None
    context = 'icon'
    if sobject_skey.find('>') >= 0:
        parts = sobject_skey.split('>')
        sobject_skey = parts[0]
        process = parts[1]
        context = parts[1] if len(parts) == 2 else '/'.join(parts[2:])

    iconss = _s.get_snapshot(sobject_skey, context=context, version=0,
            process=process)
    if not iconss.get('code'):
        return ''

    return get_snapshot_icon(iconss['code'], mode=mode, file_type=file_type)


def get_task_icon(task, mode='client_repo', file_type='icon'):
    ''' return an icon for the given task by getting icon from associated
    sobject '''
    task_skey = task
    context=None
    if isinstance(task, dict):
        task_skey = task.get('__search_key__')
    else:
        if task_skey.find('>') >= 0:
            task_skey, context = task_skey.split('>')
        try:
            task = _s.get_by_search_key(task_skey)
        except:
            task = None

    if not task_skey or not task:
        return ''

    sobject_skey = get_sobject_from_task(task)
    if not sobject_skey:
        return ''

    if context:
        process = task.get('process')
        if not process:
            process = context.split('/')[0]
        sobject_skey += '>' + process + '>' + context

    return get_sobject_icon(sobject_skey, mode=mode, file_type=file_type)


def get_path_icon(path, mode='client_repo', file_type='icon'):
    ''' return the icon for a given path in client_repo '''
    basedir = op.normcase( op.normpath(
                _s.get_base_dirs()['win32_client_repo_dir']))
    path = op.normcase(op.normpath(path))
    try:
        relative_dir = op.relpath(path, basedir)
    except ValueError:
        return ''
    relative_dir, file_name = op.split(relative_dir)
    file_sobj = _s.query('sthpw/file', filters=[ ('relative_dir',
        relative_dir.replace('\\', '/')), ('file_name', file_name)],
        single = True)
    if file_sobj:
        return get_file_icon(file_sobj, mode=mode, file_type=file_type)
    return ''


def get_file_icon(file_sobject, mode='client_repo', file_type='icon'):
    ''' get an icon from the snapshot given a file sobject '''
    sscode = file_sobject.get('snapshot_code')
    if not sscode:
        ss = _s.query('sthpw/file', filters=[('code', file_sobject['code'])],
                columns = ['snapshot_code'], single=True)
        if ss:
            sscode = ss['snapshot_code']
        else:
            return ''
    return get_snapshot_icon(sscode, mode=mode, file_type=file_type)


def get_snapshot_icon(snapshot_code, mode='client_repo', file_type='icon'):
    ''' get the icon path of the given sobject

    @param snapshot_code: sobject searchkey for which the icon is required
    @keyparam mode: the type of the path required, leave it to default
    client_repo for production utilities
    @keyparam file_type: the type of file required, only 'main', 'icon' and
    'web' are relevant
    '''
    file_types = []
    file_types.append(file_type)
    filepath = _s.get_all_paths_from_snapshot(snapshot_code, mode=mode,
            file_types=file_types)
    if filepath:
        filepath=filepath[0]
    return filepath


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
    '''
    :task: search_key of task
    '''
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

    process = _s.get_by_search_key(task)["process"]

    snapshots = [snap for snap in snapshots
                 if snap["process"] == process and snap["context"] == context]
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
    return _s.split_search_key(search_key)[1]

def get_all_users():
    return _s.query("sthpw/login")

def get_tactic_file_info():

    import imaya as mi
    reload(mi)
    tactic_raw = mi.FileInfo.get('TACTIC')

    if tactic_raw:
        tactic = json.loads(tactic_raw)
    else:
        tactic = {}

    tactic['__ver__'] = '0.2'   # more snapshot info added

    return tactic

def get_references():
    '''
    @return: dict of reference that were referenced via TACTIC
    i.e. their trace is recorded in the fileInfo ({[ref_node: ref_path[,]]})
    '''
    import imaya as mi
    reload(mi)

    refs = mi.get_reference_paths()
    t_info = get_tactic_file_info()
    snap_path = [op.normpath(get_filename_from_snap(snap, mode = 'client_repo')).lower()
                 for snap in t_info.get('assets')]
    # if the ref'ed file has no entry in the fileInfo purge it from  refs
    for ref, path in dict(**refs).iteritems():

        if op.normpath(path).lower() not in snap_path:
            refs.pop(ref)
    return refs

def set_tactic_file_info(tactic):
    '''
    @tactic: dict
    '''
    import imaya as mi
    reload(mi)
    return mi.FileInfo.save('TACTIC', json.dumps(tactic))

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

def get_snapshot_info(search_key):
    snapshot = _s.get_by_search_key(search_key)
    snapshot['asset'] = _s.get_by_code(snapshot['search_type'],
        snapshot['search_code'])
    return snapshot

def copy_snapshot(snapshot_from, snapshot_to, mode='copy'):
    ''' make a copy of all the files in the snaphot_from to snapshot_to
    '''
    server = _s
    dirs = []
    groups = []
    files = []
    ftypes = []
    base_dir = server.get_base_dirs()['win32_client_repo_dir']

    for fileEntry in server.get_all_children(snapshot_from['__search_key__'],
            'sthpw/file'):
        file_path = op.join(base_dir, fileEntry['relative_dir'],
                fileEntry['file_name']).replace('/', '\\')
        if fileEntry['base_type'] == 'file':
            files.append(file_path)
            ftypes.append(fileEntry['type'])
        elif fileEntry['base_type'] == 'directory':
            dirs.append((file_path, fileEntry['type']))
        elif fileEntry['base_type'] == 'sequence':
            groups.append((file_path, fileEntry['file_range'], fileEntry['type']))

    server.add_file(snapshot_to['code'], files, file_type=ftypes, mode=mode, create_icon=False)
    for directory in dirs:
        server.add_directory(snapshot_to['code'], directory[0],
                file_type=directory[1], mode=mode)
    for group in groups:
        server.add_group(snapshot_to['code'], group[1], group[0], mode=mode)

    return True


def get_episode_asset(project, episode, asset, force_create=False):
    proj = current_project()
    set_project(project)

    if force_create:
        obj = _s.get_unique_sobject('vfx/asset_in_episode', data={
            'episode_code': episode['code'],
            'asset_code': asset['code']})
    else:
        obj = _s.query('vfx/asset_in_episode',
                filters=[('episode_code', episode['code']),
                    ('asset_code', asset['code'])], single=True)

    set_project(proj)
    return obj


def publish_asset_to_episode(project_sk, episode, asset, snapshot, context,
        set_current=True):
    server = _s
    pub_obj = get_episode_asset(project_sk, episode, asset, True)

    newss = server.create_snapshot(pub_obj, context=context,
            is_current=set_current, snapshot_type=snapshot['snapshot_type'])

    copy_snapshot(snapshot, newss)

    server.add_dependency_by_code(newss['code'], snapshot['code'],
            type='ref', tag='publish_source')
    server.add_dependency_by_code(snapshot['code'], newss['code'],
            type='ref', tag='publish_target')

    return newss


def get_published_snapshots_in_episode(project_sk, episode, asset, context):
    pub_obj = get_episode_asset(project_sk, episode, asset)
    snapshots = []
    if pub_obj:
        snapshots = get_snapshot_from_sobject(pub_obj['__search_key__'])
    snapshots = [ss for ss in snapshots if ss['context'] == context]
    return snapshots


def get_all_publish_targets(snapshot):
    server = _s
    return server.get_dependencies(snapshot, tag='publish_target')


all_assets = get_assets
all_tasks = get_tasks

