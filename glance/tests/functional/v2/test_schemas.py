# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 OpenStack, LLC
# All Rights Reserved.
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

import requests

from glance.tests import functional


class TestSchemas(functional.FunctionalTest):

    def setUp(self):
        super(TestSchemas, self).setUp()
        self.cleanup()
        self.start_servers(**self.__dict__.copy())

    def test_resource(self):
        # Ensure the image link works and custom properties are loaded
        path = 'http://%s:%d/v2/schemas/image' % ('127.0.0.1', self.api_port)
        response = requests.get(path)
        self.assertEqual(response.status_code, 200)
        image_schema = json.loads(response.text)
        expected = set([
            'id',
            'name',
            'owner',
            'visibility',
            'checksum',
            'created_at',
            'updated_at',
            'tags',
            'size',
            'type',
            'format',
            'self',
            'file',
            'status',
            'access',
            'schema',
            'location',
        ])
        self.assertEqual(expected, set(image_schema['properties'].keys()))

        # Ensure the access link works
        path = 'http://%s:%d/v2/schemas/image/access' % \
                ('127.0.0.1', self.api_port)
        response = requests.get(path)
        self.assertEqual(response.status_code, 200)
        access_schema = json.loads(response.text)

        # Ensure the images link works and agrees with the image schema
        path = 'http://%s:%d/v2/schemas/images' % ('127.0.0.1', self.api_port)
        response = requests.get(path)
        self.assertEqual(response.status_code, 200)
        images_schema = json.loads(response.text)
        item_schema = images_schema['properties']['images']['items']
        self.assertEqual(item_schema, image_schema)

        # Ensure the accesses schema works and agrees with access schema
        path = 'http://%s:%d/v2/schemas/image/accesses' % \
                ('127.0.0.1', self.api_port)
        response = requests.get(path)
        self.assertEqual(response.status_code, 200)
        accesses_schema = json.loads(response.text)
        item_schema = accesses_schema['properties']['accesses']['items']
        self.assertEqual(item_schema, access_schema)
