import etcd3
import uuid
import requests
import json
from .exceptions import AccountNameNotFound

# settings, need to move out to config

AMS_URL = 'http://localhost:1206/api/jt-ams/v0.1'
WRS_ETCD_ROOT = '/jthub:wrs'

etcd_client = etcd3.client()


def get_account_id_by_name(account_name):
    request_url = '%s/accounts/%s' % (AMS_URL.strip('/'), account_name)
    try:
        r = requests.get(request_url)
    except:
        return

    if r.status_code != 200:
        raise AccountNameNotFound(account_name)

    return json.loads(r.text).get('_id')


def get_workflows(account_name):
    account_id = get_account_id_by_name(account_name)

    if account_id:
        workflows = {
            "account_id": account_id,
            "account_name": account_name,
            "workflows": []
        }

        key_prefix = '/'.join([WRS_ETCD_ROOT,
                               'account._id:%s' % account_id])

        # ideally we don't read values yet
        r = etcd_client.get_prefix(key_prefix=key_prefix, sort_target='KEY')

        for value, meta in r:
            k = meta.key.decode('utf-8').replace(key_prefix, '', 1)
            try:
                v = value.decode("utf-8")
            except:
                v = None  # assume binary value
            print(k, v)

            if ':' in k:
                if k.startswith('is_'):
                    v = True if k.split(':', 1)[1] else False
                else:
                    k, v = k.split(':', 1)
            else:
                if k.startswith('is_'):
                    v = True if v and v != '0' else False

        return workflows
    else:
        raise AccountNameNotFound(Exception("Specific account name not found: %s" % account_name))


def get_workflow(account_name, workflow_name):
    key = '/'.join([WRS_ETCD_ROOT, '%s:%s' % ('_name', account_name)])
    r = etcd_client.get(key)

    try:
        _id = r[0].decode("utf-8")
    except:
        return

    account = {
        '_id': _id
    }

    key_prefix = '/'.join([AMS_ROOT, ACCOUNT_PATH, 'data', '%s:%s/' % ('_id', _id)])
    r = etcd_client.get_prefix(key_prefix=key_prefix, sort_target='KEY')

    for value, meta in r:
        k = meta.key.decode('utf-8').replace(key_prefix, '', 1)
        v = value.decode("utf-8")
        if ':' in k:
            if k.startswith('is_'):
                v = True if k.split(':', 1)[1] else False
            else:
                k, v = k.split(':', 1)
        else:
            if k.startswith('is_'):
                v = True if v else False

        if not '@' in k:
            account[k] = v
        else:
            sub_key, sub_type = k.split('@', 1)
            if not sub_type in account: account[sub_type] = []
            account[sub_type].append({sub_key: v})

    return account


def register_workflow(account_name, account_type):
    _id = str(uuid.uuid4())

    key = '/'.join([AMS_ROOT, ACCOUNT_PATH, '%s:%s' % ('_name', account_name)])
    r = etcd_client.put(key, _id)

    key_prefix = '/'.join([AMS_ROOT, ACCOUNT_PATH, 'data', '%s:%s' % ('_id', _id)])
    r = etcd_client.put('%s/_name' % key_prefix, account_name)

    if account_type == 'org':
        r = etcd_client.put('%s/is_org' % key_prefix, '1')
    else:
        r = etcd_client.put('%s/is_org' % key_prefix, '')

    return get_account(account_name)


def update_account():
    pass


def delete_account():
    pass


def add_member():
    pass


def delete_member():
    pass
