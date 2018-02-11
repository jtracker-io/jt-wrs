import os
import etcd3
import zipfile
import tempfile
from io import BytesIO
import uuid
import requests
import json
from .exceptions import OwnerNameNotFound, AMSNotAvailable, OwnerIDNotFound, InvalidJTWorkflowFile
from .jtracker import JTracker


# settings, need to move out to config

ams_host = os.environ['AMS_HOST'] if os.environ.get('AMS_HOST') else 'localhost'
ams_port = os.environ['AMS_PORT'] if os.environ.get('AMS_PORT') else 12012
AMS_URL = 'http://%s:%s/api/jt-ams/v0.1' % (ams_host, ams_port)

WRS_ETCD_ROOT = '/jt:wrs'
etcd_host = os.environ['ETCD_HOST'] if os.environ.get('ETCD_HOST') else 'localhost'
etcd_port = os.environ['ETCD_PORT'] if os.environ.get('ETCD_PORT') else 2379

etcd_client = etcd3.client(host=etcd_host, port=etcd_port)


def _get_owner_id_by_name(owner_name):
    request_url = '%s/accounts/%s' % (AMS_URL.strip('/'), owner_name)
    try:
        r = requests.get(request_url)
    except:
        raise AMSNotAvailable('AMS service temporarily unavailable')

    if r.status_code != 200:
        raise OwnerNameNotFound(owner_name)

    return json.loads(r.text).get('id')


def _get_owner_name_by_id(owner_id):
    request_url = '%s/accounts/_id/%s' % (AMS_URL.strip('/'), owner_id)
    try:
        r = requests.get(request_url)
    except:
        raise AMSNotAvailable('AMS service temporarily unavailable')

    if r.status_code != 200:
        raise OwnerIDNotFound(owner_id)

    return json.loads(r.text).get('name')


def _get_workflow_id_by_owner_id_and_workflow_name(owner_id, workflow_name):
    v, meta = etcd_client.get('%s/owner.id:%s/workflow/name:%s/id' % (WRS_ETCD_ROOT, owner_id, workflow_name ))
    if v:
        return v.decode("utf-8")

def get_workflows(owner_name=None, workflow_name=None, workflow_version=None):
    owner_id = _get_owner_id_by_name(owner_name)

    if owner_id:
        workflows = []

        # find the workflows' name and id first
        workflow_name_id_prefix = '/'.join([WRS_ETCD_ROOT,
                                            'owner.id:%s' % owner_id,
                                            'workflow/'])

        if workflow_name:
            key_search_prefix = '%sname:%s/id' % (workflow_name_id_prefix, workflow_name)
        else:
            key_search_prefix = '%sname' % workflow_name_id_prefix

        r = etcd_client.get_prefix(key_prefix=key_search_prefix, sort_target='KEY')

        for value, meta in r:
            k = meta.key.decode('utf-8').replace(workflow_name_id_prefix, '', 1)
            try:
                v = value.decode("utf-8")
            except:
                v = None  # assume binary value, deal with it later

            #print("k:%s, v:%s" % (k, v))

            workflow = get_workflow_by_id_and_version(v, workflow_version, owner_name=owner_name)

            if workflow:
                workflows.append(workflow)

        return workflows
    else:
        raise OwnerNameNotFound(Exception("Specific owner name not found: %s" % owner_name))


def get_workflow_by_id_and_version(workflow_id, workflow_version=None, owner_name=None):
    workflow = {
        "id": workflow_id
    }

    # ideally we don't read values yet, but python-etcd does not have this option
    workflow_prefix = '/'.join([WRS_ETCD_ROOT, 'workflow', 'id:%s/' % workflow_id])

    r2 = etcd_client.get_prefix(key_prefix=workflow_prefix, sort_target='KEY')

    workflow_version_found = False

    for value2, meta2 in r2:
        k = meta2.key.decode('utf-8').replace(workflow_prefix, '', 1)
        try:
            v = value2.decode("utf-8")
        except:
            v = None  # assume binary value, deal with it later
        #print("k:%s, v:%s" % (k, v))
        parts = k.split('/')

        if ':' in parts[-1]:
            new_key, new_value = parts[-1].split(':', 1)
        else:
            new_key, new_value = parts[-1], v
            if new_key == 'workflowfile':
                new_value = None

        if isinstance(new_value, str):
            new_value = new_value.replace('+', '/')

        if new_key.startswith('is_'):
            new_value = True if new_value and new_value != '0' else False

        if len(parts) == 1:
            if '@' not in new_key:
                workflow[new_key] = new_value
            else:
                sub_key, sub_type = new_key.split('@', 1)
                if sub_type not in workflow:
                    workflow[sub_type] = []
                workflow[sub_type].append({sub_key: new_value})

        elif len(parts) == 2:
            ver_tag, ver = parts[0].split(':', 1)
            if workflow_version and not workflow_version == ver:
                continue
            else:
                workflow_version_found = True

            ver = 'ver:%s' % ver
            if ver_tag == 'ver' and not workflow.get(ver):
                workflow[ver] = {}

            if '@' not in new_key:
                workflow[ver][new_key] = new_value
            else:
                sub_key, sub_type = new_key.split('@', 1)
                if sub_type not in workflow[ver]: workflow[ver][sub_type] = []
                workflow[ver][sub_type].append({sub_key: new_value})

    if workflow_version_found:
        if owner_name:
            workflow['owner.name'] = owner_name
        else:
            workflow['owner.name'] = _get_owner_name_by_id(workflow.get('owner.id'))
        return workflow


def get_workflow(owner_name, workflow_name, workflow_version=None):
    workflow = get_workflows(owner_name, workflow_name, workflow_version)
    if workflow and workflow_version and 'ver:%s' % workflow_version not in workflow[0]:
        return
    elif workflow:
        return workflow[0]


def get_file(owner_name, workflow_name, workflow_version, file_type):
    if file_type not in ('workflowfile', 'workflow_package'):
        return

    owner_id = _get_owner_id_by_name(owner_name)

    if owner_id:
        workflow_id = _get_workflow_id_by_owner_id_and_workflow_name(owner_id, workflow_name)
        #print(workflow_id)
        if workflow_id:
            v, meta = etcd_client.get('%s/workflow/id:%s/ver:%s/%s' %
                                      (WRS_ETCD_ROOT, workflow_id, workflow_version, file_type))
            if v:
                return v.decode("utf-8") if file_type == 'workflowfile' else v


def get_workflowfile(owner_name, workflow_name, workflow_version):
    return get_file(owner_name, workflow_name, workflow_version, 'workflowfile')


def get_workflow_package(owner_name, workflow_name, workflow_version):
    return get_file(owner_name, workflow_name, workflow_version, 'workflow_package')


def get_jobjson_template(owner_name, workflow_name, workflow_version, jobjson):
    pass


def register_workflow(owner_name, workflow_entry):
    owner_id = _get_owner_id_by_name(owner_name)

    # lots of validation/error check need to happen, do it later
    workflow_name = workflow_entry.get('name')
    workflow_version = workflow_entry.get('version')

    # make sure same workflow name/version does not exist for the same owner
    # not to allow this for now, later will need to consider user update a previously
    # registered workflow but not yet released. Not change allowed once released
    if get_workflow(owner_name, workflow_name, workflow_version):
        raise Exception('Same workflow already registered.')

    git_server = workflow_entry.get('git_server')
    git_account = workflow_entry.get('git_account')
    git_repo = workflow_entry.get('git_repo')
    git_tag = workflow_entry.get('git_tag')
    git_path = workflow_entry.get('git_path')

    if workflow_version != git_tag and '%s.%s' % (workflow_name, workflow_version) != git_tag:
        raise Exception('Workflow version must match git tag.')

    git_download_url = "%s/%s/%s/archive/%s.zip" % (git_server, git_account,
                                                    git_repo, git_tag)

    tmp_dir = tempfile.mkdtemp()
    request = requests.get(git_download_url)
    zfile = zipfile.ZipFile(BytesIO(request.content))
    zfile.extractall(tmp_dir)

    source_workflow_path = os.path.join(tmp_dir, '%s-%s' % (git_repo, git_tag), git_path, 'workflow')

    try:  # new convention
        with open(os.path.join(source_workflow_path, 'main.yaml'), 'r') as f:
            workflow_file_yaml = f.read()
    except IOError:  # old naming of entry point workflow file
        with open(os.path.join(source_workflow_path, '%s.jt.yaml' % workflow_name), 'r') as f:
            workflow_file_yaml = f.read()

    try:  # validate workflow file by create a JT object
        jt = JTracker(workflow_yaml_string=workflow_file_yaml)
    except Exception as err:
        print(str(err))
        raise InvalidJTWorkflowFile

    # workflow entry etcd key
    # /jt:wrs/owner.id:7ebf7fa9-f70f-481a-a499-5fba3f8c5078/workflow/name:test/id
    workflow_entry_etcd_key = '%s/owner.id:%s/workflow/name:%s/id' % (WRS_ETCD_ROOT,
                                                                      owner_id,  workflow_name)
    # check whether the workflow exists already and this is to register a new version
    v, meta = etcd_client.get(workflow_entry_etcd_key)
    if v:
        workflow_id = v.decode("utf-8")
    else:
        workflow_id = str(uuid.uuid4())

    workflow_property_key_prefix = '%s/workflow/id:%s' % (WRS_ETCD_ROOT, workflow_id)

    # now write to etcd
    etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(workflow_entry_etcd_key) > 0,  # test key exists
        ],
        success=[  # this is for additional new versions of the workflow
            etcd_client.transactions.put('%s/ver:%s/%s' % (workflow_property_key_prefix,
                                                           workflow_version, 'git_path'),
                                         git_path),
            etcd_client.transactions.put('%s/ver:%s/%s' % (workflow_property_key_prefix,
                                                           workflow_version, 'git_tag'),
                                         git_tag),
            etcd_client.transactions.put('%s/ver:%s/%s' % (workflow_property_key_prefix,
                                                           workflow_version, 'workflowfile'),
                                         workflow_file_yaml)
        ],
        failure=[
            etcd_client.transactions.put(workflow_entry_etcd_key, workflow_id),
            etcd_client.transactions.put('%s/%s' % (workflow_property_key_prefix, 'workflow_type'), 'JTracker'),
            etcd_client.transactions.put('%s/%s' % (workflow_property_key_prefix, 'git_account'), git_account),
            etcd_client.transactions.put('%s/%s' % (workflow_property_key_prefix, 'git_repo'), git_repo),
            etcd_client.transactions.put('%s/%s' % (workflow_property_key_prefix, 'name'), workflow_name),
            etcd_client.transactions.put('%s/%s' % (workflow_property_key_prefix, 'owner.id'), owner_id),
            etcd_client.transactions.put('%s/ver:%s/%s' % (workflow_property_key_prefix,
                                                           workflow_version, 'git_path'),
                                         git_path),
            etcd_client.transactions.put('%s/ver:%s/%s' % (workflow_property_key_prefix,
                                                           workflow_version, 'git_tag'),
                                         git_tag),
            etcd_client.transactions.put('%s/ver:%s/%s' % (workflow_property_key_prefix,
                                                           workflow_version, 'workflowfile'),
                                         workflow_file_yaml)
        ]
    )

    return get_workflow(owner_name, workflow_name, workflow_version)


def get_execution_plan(owner_name, workflow_name, workflow_version, job_json):
    workflow = get_workflow(owner_name, workflow_name, workflow_version)
    workflowfile = get_workflowfile(owner_name, workflow_name, workflow_version)

    if workflow.get('workflow_type') == 'JTracker':
        jt = JTracker(workflow_yaml_string=workflowfile)
        return jt.get_execution_plan(job_json)
    else:
        raise NotImplementedError('Workflow types other than JTracker are not implemented yet')


def update_owner():
    pass


def delete_owner():
    pass


def add_member():
    pass


def delete_member():
    pass
