# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 OpenStack Foundation
# All Rights Reserved.
# Copyright 2013 IBM Corp.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import os

import fixtures
import requests

from glance.openstack.common import uuidutils
from glance.tests import functional


TENANT1 = uuidutils.generate_uuid()
TENANT2 = uuidutils.generate_uuid()
TENANT3 = uuidutils.generate_uuid()
TENANT4 = uuidutils.generate_uuid()


class TestTasks(functional.FunctionalTest):

    def setUp(self):
        super(TestTasks, self).setUp()
        self.cleanup()
        self.file_path = self._stash_file()
        self.api_server.deployment_flavor = 'noauth'
        self.start_servers(**self.__dict__.copy())

    def _url(self, path):
        return 'http://127.0.0.1:%d%s' % (self.api_port, path)

    def _headers(self, custom_headers=None):
        base_headers = {
            'X-Identity-Status': 'Confirmed',
            'X-Auth-Token': '932c5c84-02ac-4fe5-a9ba-620af0e2bb96',
            'X-User-Id': 'f9a41d13-0c13-47e9-bee2-ce4e8bfe958e',
            'X-Tenant-Id': TENANT1,
            'X-Roles': 'member',
        }
        base_headers.update(custom_headers or {})
        return base_headers

    def _stash_file(self):
        self.tmp_dir = self.useFixture(fixtures.TempDir()).path
        self.store_dir = os.path.join(self.tmp_dir, 'images')
        os.mkdir(self.store_dir)

        file_path = os.path.join(self.store_dir, 'foo')
        with open(file_path, 'w') as f:
            f.write('blah')
        return 'file://%s' % file_path

    def test_task_lifecycle(self):
        # Task list should be empty
        path = self._url('/v2/tasks')
        response = requests.get(path, headers=self._headers())
        self.assertEqual(200, response.status_code)
        tasks = json.loads(response.text)['tasks']
        self.assertEqual(0, len(tasks))

        # Create a task (with a deployer-defined property)
        path = self._url('/v2/tasks')
        headers = self._headers({'content-type': 'application/json'})

        data = json.dumps({"type": "import",
                           "input": {
                           "import_from": self.file_path,
                           "import_from_format": "qcow2",
                           "image_properties": {
                           'disk_format': 'vhd',
                           'container_format': 'ovf'}
                           }
                           })
        response = requests.post(path, headers=headers, data=data)
        self.assertEqual(201, response.status_code)

        # Returned task entity should have a generated id and status
        task = json.loads(response.text)
        task_id = task['id']
        checked_keys = set([u'created_at',
                            u'expires_at',
                            u'id',
                            u'input',
                            u'owner',
                            u'schema',
                            u'self',
                            u'status',
                            u'type',
                            u'updated_at'])
        self.assertEqual(set(task.keys()), checked_keys)
        expected_task = {
            'status': 'pending',
            'type': 'import',
            'input': {
            "import_from": self.file_path,
            "import_from_format": "qcow2",
            "image_properties": {
                'disk_format': 'vhd',
                'container_format': 'ovf'
            }},
            'schema': '/v2/schemas/task',
        }
        for key, value in expected_task.items():
            self.assertEqual(task[key], value, key)

        # Image list should now have one entry
        path = self._url('/v2/tasks')
        response = requests.get(path, headers=self._headers())
        self.assertEqual(200, response.status_code)
        tasks = json.loads(response.text)['tasks']
        self.assertEqual(1, len(tasks))
        self.assertEqual(tasks[0]['id'], task_id)

        self.stop_servers()
