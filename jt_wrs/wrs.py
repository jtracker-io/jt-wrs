import etcd3
import uuid
import requests
import json
from .exceptions import OwnerNameNotFound, AMSNotAvailable, OwnerIDNotFound
from .jtracker import JTracker


# settings, need to move out to config

AMS_URL = 'http://localhost:12012/api/jt-ams/v0.1'
WRS_ETCD_ROOT = '/jthub:wrs'

etcd_client = etcd3.client()


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
    return "Not implemented yet"


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
