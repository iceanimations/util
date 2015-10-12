'''
This module can work both on server and client side.
'''

from site import addsitedir as asd
asd(r"r:/Pipe_Repo/Users/Hussain/utilities/TACTIC")
from datetime import datetime
import tempfile
import hashlib
import json, os
import shutil
import sys
op = os.path

_s = None
def set_server(server):
    global _s
    _s = server

_user = None
def set_user(user):
    global _user
    _user = user

try:
    from auth import user as USER
    set_user(USER.get_user())
    set_server(USER.get_server())
except Exception as e:
    _user = None
    #from tactic_client_lib import TacticServerStub
    #set_server(TacticServerStub.get(setup=False))
    from auth import user as USER
    set_server(USER.TacticServer(setup=False))


class Server(object):
    def __init__(self, server=None):
        self._server = server if server else _s

    def __get__(self, obj, cls=None):
        return self._server if self._server else _s

    def __set__(self, obj, server):
        self._server = server if server else _s

class TacticAppUtils(object):
    server = None

    def __init__(self, server=None):
        self.server = server if server else _s

    if _s and _user:
        def get_all_task(self, user = _user):
            '''
            :return: list of task search keys
            '''
            user = USER.get_user()
            return [task.get("__search_key__", '') for proj in self.get_all_projects()
                    for task in self.all_tasks(proj, user = user)]

        def get_all_project_user_sobjects(self, user = _user):
            '''

            '''
            user = USER.get_user()
            proj_dict = {}
            for proj in self.get_all_projects():
                key = proj['code']
                sobj = self.get_user_related_sobjects(proj, user)
                if sobj:
                    proj_dict[key] = sobj

            return proj_dict

        def get_user_related_sobjects(self, proj, user = _user):
            '''
            @return: sobject_search_key list
            '''
            user = USER.get_user()
            return self.map_tasks_to_sobjects(
                [task
                for task in self.all_tasks(proj, user = user)
                if str(task["status"]).lower() != "done"]).keys()

        def sobject_to_user_task(self, sobj, user = _user):
            user = USER.get_user()
            proj = self.server.get_project()
            start = sobj.find('project=')
            project = sobj[start:start + sobj[start:].find('&')]
            self.set_project(project = project.replace("project=", ""))
            tasks = self.server.get_all_children(sobj, "sthpw/task",
                                        filters = [("assigned", str(user))])
            self.set_project(project = proj)
            return [task.get('__search_key__') for task in tasks]

    def checkout(self, *args, **kargs):
        ''' a wrapper over server.checkout that can checkout versionless files in
        copy mode '''
        server = self.server

        versionless = kargs.pop('versionless', False)
        if not versionless:
            return server.checkout(*args, **kargs)

        context = kargs.get('context', 'publish')
        version = kargs.get('version', -1)
        file_types = kargs.get('file_type', [])
        to_dir = kargs.get('to_dir', '.')
        to_sandbox_dir= kargs.get('to_sandbox_dir', False)

        assert version in (0,-1)
        snapshot = server.get_snapshot(args[0], context=context,
                version=version, versionless=versionless)
        sources = server.get_all_paths_from_snapshot(snapshot['code'],
                file_types=file_types, mode='client_repo')

        if to_sandbox_dir:
            destinations = server.get_all_paths_from_snapshot(snapshot['code'],
                    file_types=file_types, mode='sandbox')
        else:
            destinations = [os.path.join(to_dir, os.path.basename(source))
                    for source in sources]

        def mkdir(path, mode=0777):

            parent=os.path.dirname(path)
            if parent and not os.path.exists(parent):
                mkdir(parent, mode=mode)

            try:
                os.mkdir(path, mode)
            except Exception:
                return False

            return True

        paths = []
        for src, dst in zip(sources, destinations):
            if os.path.isdir(src):
                shutil.copytree(src, dst)
            elif os.path.isfile(src):
                mkdir(os.path.dirname(dst))
                shutil.copy2(src, dst)
            paths.append(dst)

        return paths


    def all_task_processes(self, project, tasks = None):
        processes = set([task["process"] for task in (self.all_tasks(project)
                                                    if not tasks else tasks)])
        return list(processes)

    def all_process_tasks(self, project, process = None, tasks = None):
        processes = set([task["process"] for task in (self.all_tasks(project)
                                                    if not tasks else tasks)])
        tasks = {}
        for proc in list(processes):
            # get rid of this call to all_tasks
            t = self.all_tasks(project, proc)
            tasks[proc] = t

        return tasks[process] if process else tasks

    def current_project(self, ):
        return self.server.get_project()

    def cacheable(self, obj):
        '''
        :return: True if the object belongs to a TACTIC referenced file, else False
        :obj: Maya object
        '''
        import imaya as mi
        reload(mi)
        print mi.pc.PyNode(obj).referenceFile()
        print self.get_references().keys()
        return True if mi.pc.PyNode(obj).referenceFile() in self.get_references().keys() else False

    def date_str_to_datetime(self, string, format = "%Y-%m-%d %H:%M:%S"):
        return datetime.strptime(string.split(".")[0], format)

    def get_filename_from_snap(self, snap, mode = 'sandbox'):
        '''
        @snap: db dict
        '''
        self.set_project(project = snap['project_code'])
        path = self.server.get_all_paths_from_snapshot(snap['__search_key__'],
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
    def get_project_from_search_key(self, s_key):

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
    def get_server(self, ):
        return self.server

    def get_all_projects(self, ):
        project = self.server.get_project()
        self.set_project(project = "sthpw")
        projects = self.server.query("sthpw/project")
        self.set_project(project = project)
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

    def get_sobject_from_task(self, task):

        '''
        :task: task dict or task string
        :return: search_key of the associated sobject
        '''
        if not isinstance(task, dict):
            task = self.server.get_by_search_key(task)

        search_type = task["search_type"]
        if not search_type: return None
        return self.server.build_search_key(search_type[:search_type.find("?")
                                                if not -1
                                                else len(search_type)],
                                    task["search_code"],
                                    project_code = task.get("project_code"))

    def get_project_from_task(self, task):

        return self.server.get_by_search_key(task)['project_code']

    def get_snapshot_from_sobject(self, sobj):
        '''
        :sobj: search key of the sobj
        '''

        proj = self.server.get_project()
        self.set_project(search_key = sobj)
        snapshots =  self.server.get_all_children(sobj, "sthpw/snapshot")

        for index in reversed(range(len(snapshots))):

            if 'icon' in [snapshots[index].get(key).lower()
                        for key in ['process', 'context']]:
                snapshots.pop(index)

        self.set_project(project=proj)
        return snapshots

    def get_sobject_from_snap(self, snap):
        '''
        @snap: snap dict or snap string
        '''
        if not isinstance(snap, dict):
            snap = self.server.get_by_search_key(snap)

        search_type = snap["search_type"]
        if not search_type: return None
        return self.server.build_search_key(search_type[:search_type.find("?")
                                            if not -1
                                            else len(search_type)],
                                snap["search_code"],
                                project_code = snap.get("project_code"))

    def get_sobject_name(self, sobj):

        sobj_dict =  self.server.get_by_search_key(sobj)
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
    def get_tasks(self, project, process = None, user = None, clean = False):

        '''
        @project: takes in the project code
        @process: take in a single process whose tasks shall be returned. None implies all.
        '''

        proj = self.current_project()
        self.set_project(project = project["code"])
        filters = [("process", process)] if process else []
        filters.append(("project_code", project["code"]))

        if user is True:
            filters.append(("assigned", self.server.get_login()))
        elif user:
            filters.append(("assigned", user))
        else:
            pass

        tasks = []
        #print "project:", self.server.get_project()
        try:
            tasks = self.server.query("sthpw/task", filters = filters)
            if clean:
                pop = [drat for drat in range(len(tasks))
                    if "?project=%s" %project not in
                    str(tasks[drat]["search_type"])]
                map(tasks.pop, reversed(pop))
        except:
            print 'ERROR!!!!'
            pass

        self.pretty_print(tasks)
        self.set_project(project = proj)

        return tasks

    all_tasks = get_tasks

    def get_episodes(self, project):
        '''
        @project: project search key
        @return: list of all episode db dicts
        '''
        proj = self.current_project()
        self.set_project(project = project)
        result = self.server.query('vfx/episode')
        self.set_project(project=proj)
        return result

    def get_sequences(self, project, episode = None):
        '''
        @project: project search key
        @episode: episode search key
        @return: list of filtered sequence db dicts
        '''
        proj = self.current_project()
        self.set_project(project = project)
        result = self.server.query('vfx/sequence',
                        filters = [('episode_code', self.get_search_key_code(episode))]
                        if episode else [])
        self.set_project(project = proj)
        return result

    def get_shots(self, project, sequence = None, episode=None):
        '''
        @project: project search key
        @sequence: sequence search key
        @return: list of filtered shot db dicts
        '''
        proj = self.current_project()
        self.set_project(project = project)
        result = self.server.query('vfx/shot',
                        filters = [('sequence_code', self.get_search_key_code(sequence)
                                    if sequence else
                                    ([seq.get('code')
                                        for seq in self.server.query(
                                                'vfx/sequence', filters = [
                                                    ('episode_code',
                                                    self.get_search_key_code(episode))])]
                                    if episode else ''))]
                        if (sequence or episode) else [])
        self.set_project(project = proj)
        return result


    def get_assets(self, project, add_icons=False):
        ''' get all `assets` in a vfx project 
        @param project: code of the project being queried
        @type project: string
        @param add_icons: set this to true if you want icon paths as a list in the
        dictionary accessible by the key 'icon'. Use this with caution because it
        might slow the call down if there are a lot of assets.

        @returns: sobject dictionary
        @rtype: dict
        '''
        proj = self.current_project()
        self.set_project(project = project)
        result =  self.server.query('vfx/asset')
        if add_icons:
            for asset in result:
                asset['icon'] = self.get_icon(asset)
        self.set_project(project = proj)
        return result

    all_assets = get_assets

    _icon_cache_dir = op.join(tempfile.gettempdir(), 'tactic_icon_cache')
    if not op.exists(_icon_cache_dir):
        os.mkdir(_icon_cache_dir)

    def get_assets_in_shot(self, project, shot):
        '''
        @project: project search key
        @shot: shot search key
        @return: list of asset db dicts
        '''
        proj = self.current_project()
        self.set_project(project = project)
        result = map(self.server.get_by_search_key,
                    map(lambda code: self.server.build_search_key('vfx/asset',
                                                        code['asset_code'],
                                                        project_code = project),
                        self.server.query('vfx/asset_in_shot',
                                filters = [('shot_code',
                                            self.server.get_by_search_key(shot)['code'])],
                                columns = ['asset_code'])))
        self.set_project(proj)
        return result

    def get_project_title(self, proj_code):
        result = self.server.query("sthpw/project", filters = [("code", proj_code)])
        if result:
            return result[0]["title"]
        else:
            return None

    def get_task_process(self, task):
        return self.server.get_by_search_key(task)["process"]

    def get_contexts_from_task(self, task):
        '''
        :task: search_key of task
        '''
        sobj = self.get_sobject_from_task(task)
        task_sobj = self.server.get_by_search_key(task)
        contexts = set([item['context']
                        for item in self.get_snapshot_from_sobject(sobj)
                        if item["process"].lower() == task_sobj["process"]])

        return list(contexts) if contexts else [task_sobj['process']]

    def get_sobject_description(self, sobj):
        return "description for sobject: " +  sobj

    def get_snapshots(self, context, task):
        '''
        @context: context of the task
        @task: search_key of the task
        @return: {snapshot_search_key: {'filename': file basename at sandbox (str),
                                        'version': snapshot version (int),
                                        'latest': is this version latest (bool),
                                        'description': description of the snapshot}
        '''
        import os.path as op
        sobj = self.get_sobject_from_task(task)

        snapshots = self.get_snapshot_from_sobject(sobj)

        process = self.server.get_by_search_key(task)["process"]

        snapshots = [snap for snap in snapshots
                    if snap["process"] == process and snap["context"] == context]
        snapshot_dict = {}

        for snap in snapshots:
            snapshot_dict[snap["__search_key__"]] = {"filename": op.basename(
                self.filename_from_snap(snap)),
                                                    "version": snap["version"],
                                                    "latest": snap["is_latest"],
                                                    "description": snap[
                                                        'description'],
                                                    'timestamp': snap['timestamp']
            }
        return snapshot_dict

    def get_project_snapshots(self, proj):
        return self.server.query('sthpw/snapshot', filters = [('project_code', proj)])

    def get_search_key_code(self, search_key):
        '''
        @search_key: sobject search_key
        @return: code
        '''
        return self.server.split_search_key(search_key)[1]

    def get_all_users(self, ):
        return self.server.query("sthpw/login")

    def get_tactic_file_info(self, ):

        import imaya as mi
        reload(mi)
        tactic_raw = mi.FileInfo.get('TACTIC')

        if tactic_raw:
            tactic = json.loads(tactic_raw)
        else:
            tactic = {}

        tactic['__ver__'] = '0.2'   # more snapshot info added

        return tactic

    def get_references(self, ):
        '''
        @return: dict of reference that were referenced via TACTIC
        i.e. their trace is recorded in the fileInfo ({[ref_node: ref_path[,]]})
        '''
        import imaya as mi
        reload(mi)

        refs = mi.get_reference_paths()
        t_info = self.get_tactic_file_info()
        snap_path = [op.normpath(self.get_filename_from_snap(snap, mode = 'client_repo')).lower()
                    for snap in t_info.get('assets')]
        # if the ref'ed file has no entry in the fileInfo purge it from  refs
        for ref, path in dict(**refs).iteritems():

            if op.normpath(path).lower() not in snap_path:
                refs.pop(ref)
        return refs

    def set_tactic_file_info(self, tactic):
        '''
        @tactic: dict
        '''
        import imaya as mi
        reload(mi)
        return mi.FileInfo.save('TACTIC', json.dumps(tactic))

    def map_sobject_to_snap(self, sobjs):
        sobj_map = {}
        for sobj in sobjs:
            try:
                sobj_map[sobj] = self.get_snapshot_from_sobject(sobj)
            except:
                sobj_map[sobj] = []
        return sobj_map

    def map_tasks_to_sobjects(self, tasks):

        '''
        @return: {sobject_search_key: [tasks_database_dicts,...]}
        '''
        s_tasks = {}
        for task in tasks:

            sobj = self.get_sobject_from_task(task)
            if not sobj: continue
            try:
                s_tasks[sobj].append(task)
            except:
                s_tasks[sobj] = [task]
        return s_tasks

    def production_dept(self, project):
        pass

    def project_start_date(self, project):
        pass

    def project_code_to_title(self, proj_code):
        '''
        @return: the project title given it's code. None if the project code is not listed.
        '''
        return self.server.query("sthpw/project", [("code", proj_code)], single = True).get("title")

    def project_from_tasks(self, tasks):
        '''
        @tasks: task database column dict
        '''
        return set([task["project_code"] for task in tasks])

    def pretty_print(self, obj, ind = 2):
        import json
        print json.dumps(obj, indent = ind)

    def set_project(self, project = None, search_key = None):
        server = self.server
        if project and project != server.get_project():
            server.set_project(project)
        elif search_key:
            server.set_project(self.get_project_from_search_key(search_key))

    def task_details(self, project):
        pass

    def team_involved(self, project):
        pass

    def get_snapshot_info(self, search_key):
        snapshot = self.server.get_by_search_key(search_key)
        snapshot['asset'] = self.server.get_by_code(snapshot['search_type'],
            snapshot['search_code'])
        return snapshot

    def copy_snapshot(self, snapshot_from, snapshot_to, mode='copy'):
        ''' make a copy of all the files in the snaphot_from to snapshot_to
        '''
        server = self.server
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


    ###############################################################################
    #                                    icons                                    #
    ###############################################################################

    _memory_icon_cache = {}
    def get_cached_icon(self, obj, default=None):
        md5 = hashlib.md5(obj).hexdigest()
        iconpath = op.join(self._icon_cache_dir, md5)
        if self._memory_icon_cache.has_key(obj):
            return self._memory_icon_cache[obj]
        elif op.exists(iconpath) and op.isfile(iconpath):
            self._memory_icon_cache[obj]=iconpath
            return iconpath
        return default

    def cache_icon(self, obj, path):
        md5 = hashlib.md5(obj).hexdigest()
        iconpath = op.join(self._icon_cache_dir, md5)
        value = iconpath
        if op.exists(path) and op.isfile(path):
            if op.exists(iconpath):
                os.remove(iconpath)
            shutil.copyfile(path, iconpath)
            self._memory_icon_cache[obj]=iconpath
        else:
            self._memory_icon_cache[obj]=path
            value = path
        return value

    def get_icon(self, obj, mode='client_repo', file_type='icon'):
        ''' Get an icon for file, path, snapshot or sobject

        :param obj: filepath or skey or sobject dict of file, snapshot or sobject
        for which icon is required
        :type obj: dict or str or unicode
        '''
        iconpath = ''
        try:
            if isinstance(obj, dict):
                obj = obj.get('__search_key__')
            iconpath = self.get_cached_icon(obj)
            if iconpath is not None:
                return iconpath

            stype, code = self.server.split_search_key(obj)
            if obj.startswith('sthpw/file'):
                iconpath = self.get_file_icon(obj, mode=mode, file_type=file_type)
            elif obj.startswith('sthpw/snapshot'):
                iconpath = self.get_snapshot_icon(obj, mode=mode, file_type=file_type)
            elif obj.startswith('sthpw/task'):
                iconpath = self.get_task_icon(obj, mode=mode, file_type=file_type)
            else:
                iconpath = self.get_sobject_icon(obj, mode=mode, file_type=file_type)
        except (AssertionError, ValueError, KeyError, AttributeError):
            print 'getting path icon'
            iconpath = self.get_path_icon(obj, mode=mode, file_type=file_type)
        return self.cache_icon(obj, iconpath)

    def get_sobject_icon(self, sobject_skey, mode='client_repo', file_type='icon'):
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

        iconss = self.server.get_snapshot(sobject_skey, context=context, version=0,
                process=process)
        if not iconss.get('code'):
            return ''

        return self.get_snapshot_icon(iconss['code'], mode=mode, file_type=file_type)

    def get_task_icon(self, task, mode='client_repo', file_type='icon'):
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
                task = self.server.get_by_search_key(task_skey)
            except:
                task = None

        if not task_skey or not task:
            return ''

        sobject_skey = self.get_sobject_from_task(task)
        if not sobject_skey:
            return ''

        if context:
            process = task.get('process')
            if not process:
                process = context.split('/')[0]
            sobject_skey += '>' + process + '>' + context

        return self.get_sobject_icon(sobject_skey, mode=mode, file_type=file_type)


    def get_path_icon(self, path, mode='client_repo', file_type='icon'):
        ''' return the icon for a given path in client_repo '''
        basedir = op.normcase( op.normpath(
                    self.server.get_base_dirs()['win32_client_repo_dir']))
        path = op.normcase(op.normpath(path))
        try:
            relative_dir = op.relpath(path, basedir)
        except ValueError:
            return ''
        relative_dir, file_name = op.split(relative_dir)
        file_sobj = self.server.query('sthpw/file', filters=[ ('relative_dir',
            relative_dir.replace('\\', '/')), ('file_name', file_name)],
            single = True)
        if file_sobj:
            return self.get_file_icon(file_sobj, mode=mode, file_type=file_type)
        return ''


    def get_file_icon(self, file_sobject, mode='client_repo', file_type='icon'):
        ''' get an icon from the snapshot given a file sobject '''
        sscode = file_sobject.get('snapshot_code')
        if not sscode:
            ss = self.server.query('sthpw/file', filters=[('code', file_sobject['code'])],
                    columns = ['snapshot_code'], single=True)
            if ss:
                sscode = ss['snapshot_code']
            else:
                return ''
        return self.get_snapshot_icon(sscode, mode=mode, file_type=file_type)


    def get_snapshot_icon(self, snapshot_code, mode='client_repo', file_type='icon'):
        ''' get the icon path of the given sobject

        @param snapshot_code: sobject searchkey for which the icon is required
        @keyparam mode: the type of the path required, leave it to default
        client_repo for production utilities
        @keyparam file_type: the type of file required, only 'main', 'icon' and
        'web' are relevant
        '''
        file_types = []
        file_types.append(file_type)
        filepath = self.server.get_all_paths_from_snapshot(snapshot_code, mode=mode,
                file_types=file_types)
        if filepath:
            filepath=filepath[0]
        return filepath


    ###############################################################################
    #                      Publishing and Production Assets                       #
    ###############################################################################


    def get_episode_asset(self, project, episode, asset, force_create=False):
        proj = self.current_project()
        self.set_project(project)

        if force_create:
            obj = self.server.get_unique_sobject('vfx/asset_in_episode', data={
                'episode_code': episode['code'],
                'asset_code': asset['code']})
        else:
            obj = self.server.query('vfx/asset_in_episode',
                    filters=[('episode_code', episode['code']),
                        ('asset_code', asset['code'])], single=True)

        self.set_project(proj)
        return obj

    def get_sequence_asset(self, project, sequence, asset, force_create=False):
        proj = self.current_project()
        self.set_project(project)

        if force_create:
            obj = self.server.get_unique_sobject('vfx/asset_in_sequence', data={
                'sequence_code': sequence['code'],
                'asset_code': asset['code']})
        else:
            obj = self.server.query('vfx/asset_in_sequence', filters=[('sequence_code',
                sequence['code']), ('asset_code', asset['code'])], single=True)

        self.set_project(proj)
        return obj

    def get_shot_asset(self, project, shot, asset, force_create=False):
        proj = self.current_project()
        self.set_project(project)

        if force_create:
            obj = self.server.get_unique_sobject('vfx/asset_in_shot', data={
                'shot_code': shot['code'],
                'asset_code': asset['code']})
        else:
            obj = self.server.query('vfx/asset_in_shot', filters=[('shot_code',
                shot['code']), ('asset_code', asset['code'])], single=True)

        self.set_project(proj)
        return obj

    def get_production_asset(self, project, prod_elem, asset, force_create=False):
        server = self.server
        search_type = server.split_search_key(prod_elem['__search_key__'])[0]
        func = None
        if search_type.startswith('vfx/episode'):
            func = self.get_episode_asset
        elif search_type.startswith('vfx/sequence'):
            func = self.get_sequence_asset
        elif search_type.startswith('vfx/shot'):
            func = self.get_shot_asset
        return func(project, prod_elem, asset, force_create)



    prod_elem_stypes = {}
    prod_elem_stypes['vfx/episode'] = ('vfx/asset_in_episode', 'episode_code',
            "@SOBJECT(vfx/asset_in_episode['episode_code', '%s'].vfx/asset)")
    prod_elem_stypes['vfx/sequence'] = ('vfx/asset_in_sequence', 'sequence_code',
            "@SOBJECT(vfx/asset_in_sequence['sequence_code', '%s'].vfx/asset)")
    prod_elem_stypes['vfx/shot'] = ('vfx/asset_in_shot', 'shot_code',
            "@SOBJECT(vfx/asset_in_shot['shot_code', '%s'].vfx/asset)")

    def get_production_assets(self, project, prod_elem, expand_assets=True):
        server = self.server
        search_type = server.split_search_key(prod_elem['__search_key__'])[0]
        proj = server.get_project()
        server.set_project(project)
        prod_assets = []

        for stype, (prod_stype, code_key, tel) in self.prod_elem_stypes.items():
            if search_type.startswith(stype):
                prod_assets = server.query(prod_stype, filters=[(code_key,
                    prod_elem['code'])])
                if prod_assets and expand_assets:
                    assets = {a['code']:a for a in
                            server.eval(tel%prod_elem['code'])}
                for prod in prod_assets:
                    prod['asset'] = assets.get(prod['asset_code'])

        if proj:
            server.set_project(proj)
        return prod_assets

    def is_production_asset_paired(self, prod_asset):
        prod_snapshots = self.get_snapshot_from_sobject(prod_asset['__search_key__'])
        rigs = [snap for snap in prod_snapshots if snap['context'] == 'rig']
        shadeds = [snap for snap in prod_snapshots if snap['context'] == 'shaded']

        def get_current(snaps):
            for snap in snaps:
                if snap['is_current']:
                    return snap

        current_rig = get_current(rigs)
        current_shaded = get_current(shadeds)

        if not (current_rig and current_shaded):
            return False

        source_rig = self.get_publish_source(current_rig)
        source_shaded = self.get_publish_source(current_shaded)

        return self.is_cache_compatible(source_shaded, source_rig)


    def publish_asset_to_episode(self, project, episode, asset, snapshot, context,
            set_current=True):
        server = self.server
        pub_obj = self.get_episode_asset(project, episode, asset, True)

        newss = server.create_snapshot(pub_obj, context=context,
                is_current=set_current, snapshot_type=snapshot['snapshot_type'])

        self.copy_snapshot(snapshot, newss)

        server.add_dependency_by_code(newss['code'], snapshot['code'],
                type='ref', tag='publish_source')
        server.add_dependency_by_code(snapshot['code'], newss['code'],
                type='ref', tag='publish_target')

        return newss

    def publish_asset_to_sequence(self, project, sequence, asset, snapshot, context,
            set_current=True):
        server = self.server
        pub_obj = self.get_sequence_asset(project, sequence, asset, True)

        newss = server.create_snapshot(pub_obj, context=context,
                is_current=set_current, snapshot_type=snapshot['snapshot_type'])

        self.copy_snapshot(snapshot, newss)

        server.add_dependency_by_code(newss['code'], snapshot['code'], type='ref',
                tag='publish_source')
        server.add_dependency_by_code(snapshot['code'], newss['code'], type='ref',
                tag='publish_target')

        return newss

    def publish_asset_to_shot(self, project, shot, asset, snapshot, context,
            set_current=True):
        server = self.server
        pub_obj = self.get_shot_asset(project, shot, asset, True)

        newss = server.create_snapshot(pub_obj, context=context,
                is_current=set_current, snapshot_type=snapshot['snapshot_type'])

        self.copy_snapshot(snapshot, newss)

        server.add_dependency_by_code(newss['code'], snapshot['code'], type='ref',
                tag='publish_source')
        server.add_dependency_by_code(snapshot['code'], newss['code'], type='ref',
                tag='publish_target')

        return newss

    def publish_asset(self, project, prod_elem, asset, snapshot, context,
            set_current=True):
        server = self.server
        pub_obj = self.get_production_asset(project, prod_elem, asset,
                force_create=True)

        newss = server.create_snapshot(pub_obj, context=context,
                is_current=set_current, snapshot_type=snapshot['snapshot_type'])

        self.copy_snapshot(snapshot, newss)
        self.add_publish_dependency(snapshot, newss)

        return newss

    def get_published_snapshots_in_episode(self, project_sk, episode, asset,
            context=None):
        pub_obj = self.get_episode_asset(project_sk, episode, asset)
        snapshots = []
        if pub_obj:
            snapshots = self.get_snapshot_from_sobject(pub_obj['__search_key__'])
        if context is not None:
            snapshots = [ss for ss in snapshots if ss['context'] == context]
        return snapshots

    def get_published_snapshots_in_sequence(self, project_sk, sequence, asset, context=None):
        pub_obj = self.get_sequence_asset(project_sk, sequence, asset)
        snapshots = []
        if pub_obj:
            snapshots = self.get_snapshot_from_sobject(pub_obj['__search_key__'])
        if context is not None:
            snapshots = [ss for ss in snapshots if ss['context'] == context]
        return snapshots

    def get_published_snapshots_in_shot(self, project_sk, shot, asset, context=None):
        pub_obj = self.get_shot_asset(project_sk, shot, asset)
        snapshots = []
        if pub_obj:
            snapshots = self.get_snapshot_from_sobject(pub_obj['__search_key__'])
        if context is not None:
            snapshots = [ss for ss in snapshots if ss['context'] == context]
        return snapshots

    def get_published_snapshots(self, project_sk, prod_elem, asset, context=None):
        pub_obj = self.get_production_asset(project_sk, prod_elem, asset)
        snapshots = []
        if pub_obj:
            snapshots = self.get_snapshot_from_sobject(pub_obj['__search_key__'])
        if context is not None:
            snapshots = [ss for ss in snapshots if ss['context'] == context]
        return snapshots


    ###############################################################################
    #                                  textures                                   #
    ###############################################################################

    def get_texture_snapshot(self, asset, snapshot, version=-1, versionless=False):
        server = self.server
        if not isinstance(asset, dict):
            asset = server.get_by_search_key(asset)
        texture_context = self.get_texture_context(snapshot)
        textures = server.get_all_children(asset['__search_key__'], 'vfx/texture')
        texture_snap = {}
        if textures:
            texture_child = textures[0]
            texture_snap = server.get_snapshot(texture_child['__search_key__'],
                    context = texture_context, version=version,
                    versionless=versionless)
        return texture_snap

    def get_texture_by_dependency(self, snapshot):
        texture = {}
        rootContext = snapshot['context'].split('/')[0]
        if rootContext == 'shaded':
            deps = self.get_dependencies(snapshot, keyword='texture',
                    source=False)[0]
            try:
                texture = deps[0]
            except:
                pass
        return texture

    def get_published_texture_snapshot(self, prod_asset, snapshot, version=0,
            versionless=False):
        server = self.server
        texture_context = self.get_texture_context(snapshot)
        print texture_context, version, versionless, prod_asset['__search_key__']
        return server.get_snapshot(prod_asset['__search_key__'],
                context=texture_context, version=version, versionless=versionless)

    def get_texture_context(self, snapshot):
        context = snapshot['context'].split('/')
        if context[0] != 'shaded':
            return
        texture_context = '/'.join(['texture'] + context[1:])
        return texture_context


    ###############################################################################
    #                                 dependencies                                #
    ###############################################################################

    dependency_tags_map = {
            'publish': ('source', 'target'),
            'texture': ('model', 'images'),
            'combined': ('separate', 'combined'),
            'cache': ('compatible_shaded', 'compatible_rig'),
            'default': ('forward', 'backward'),
            'mayagpucache': ('mayafile', 'gpucache')
    }

    def get_dependency_tags(self, keyword='default'):
        default = self.dependency_tags_map['default']
        source_tag, target_tag = self.dependency_tags_map.get(keyword, default)
        source_tag = '_'.join([keyword, source_tag])
        target_tag = '_'.join([keyword, target_tag])
        return source_tag, target_tag

    def add_dependency(self, source, target, keyword='default'):
        server = self.server
        source_tag, target_tag = self.get_dependency_tags(keyword)
        server.add_dependency_by_code(target['code'], source['code'], type='ref',
                tag=source_tag)
        server.add_dependency_by_code(source['code'], target['code'], type='ref',
                tag=target_tag)
        return True

    def get_dependencies(self, snapshot, keyword='default', source=True):
        server = self.server
        source_tag, target_tag = self.get_dependency_tags(keyword)
        return server.get_dependencies(snapshot, tag=source_tag if source else target_tag)

    def add_publish_dependency(self, source, target):
        return self.add_dependency(source, target, keyword='publish')

    def add_texture_dependency(self, shaded, texture):
        return self.add_dependency(shaded, texture, keyword='texture')

    def link_shaded_to_rig(self, shaded, rig):
        return self.add_dependency(shaded, rig, keyword='cache')
    add_cache_dependency = link_shaded_to_rig

    def add_combined_dependency(self, separate, combined):
        return self.add_dependency(separate, combined, keyword='combined')

    def get_combined_version(self, snapshot):
        dep = self.get_dependencies(snapshot, keyword='combined', source=False)
        return dep[0] if dep else {}

    def get_separate_version(self, snapshot):
        dep = self.get_dependencies(snapshot, keyword='combined', source=True)
        return dep[0] if dep else {}

    def get_publish_source(self, snapshot):
        dep = self.get_dependencies(snapshot, keyword='publish', source=True)
        return dep[0] if dep else {}

    def get_all_publish_targets(self, snapshot):
        return self.get_dependencies(snapshot, keyword='publish', source=False)

    def get_published_targets_in_episode(self, snapshot, project_sk, episode):
        server = self.server
        asset = snapshot.get('asset')
        if not asset:
            asset = server.get_parent(snapshot)
        pub_obj = self.get_episode_asset(project_sk, episode, asset)
        targets = self.get_all_publish_targets(snapshot)
        published = self.get_snapshot_from_sobject(pub_obj['__search_key__'])
        result = [target for target in targets for pub in published if
                target['code'] == pub['code']]
        return result

    def get_published_targets(self, snapshot, project, prod_elem):
        server = self.server
        asset = snapshot.get('asset')
        if not asset:
            asset = server.get_parent(snapshot)
        pub_obj = self.get_episode_asset(project, prod_elem, asset)
        targets = self.get_all_publish_targets(snapshot)
        published = self.get_snapshot_from_sobject(pub_obj['__search_key__'])
        result = [target for target in targets for pub in published if
                target['code'] == pub['code']]
        return result

    def get_cache_compatible_objects(self, snapshot):
        compatible = []
        if snapshot['context'].split('/')[0] == 'rig':
            compatible = self.get_dependencies(snapshot, keyword='cache', source=True)
        elif snapshot['context'].split('/')[0] == 'shaded':
            compatible = self.get_dependencies(snapshot, keyword='cache', source=False)
        return compatible

    def is_cache_compatible(self, shaded, rig):
        compatible = self.get_cache_compatible_objects(shaded)
        for obj in compatible:
            if rig['code'] == obj['code']:
                return True
        return False

    @classmethod
    def create_new(cls, server=None, force=True):
        if not server and force:
            server = USER.get_server_copy()
        return TacticAppUtils(server)

    def dummy(self):
        pass


module = sys.modules[__name__]
# make a default object
main = TacticAppUtils()
for name, val in TacticAppUtils.__dict__.items():
    obj = getattr(main, name)
    if type(obj) == type(TacticAppUtils.dummy):
        setattr(module, name, obj)

