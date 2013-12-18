
# Copyright 2013 Rackspace
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

import uuid

import mock

from glance.common import exception
from glance.common.scripts.image_import import import_swift_to_swift
from glance.tests.unit import base
import glance.tests.unit.utils as unit_utils


UUID1 = 'c80a1a6c-bd1f-41c5-90ee-81afedb1d58d'
TENANT1 = '6838eb7b-6ded-434a-882c-b344c77fe8df'


class FakeTaskRepo(object):
    def __init__(self, fake_task=None, result=None):
        self.result = result
        self.fake_task = fake_task
        self.request = unit_utils.get_fake_request()

    def get(self, task_id):
        if isinstance(self.result, BaseException):
            raise self.result
        else:
            return self.fake_task

    def save(self, task):
        self.saved_task = task


class FakeImageRepo(object):
    def __init__(self):
        pass

    def add(self, image):
        pass

    def save(self, image):
        pass

    def get(self, image_id):
        pass


class FakeImageFactory(object):
    def __init__(self, fake_image):
        self.fake_image = fake_image

    def new_image(self, *args, **kwargs):
        return self.fake_image


class FakeGateway(object):
    def __init__(self, task_repo, image_repo, image_factory):
        self.task_repo = task_repo
        self.image_repo = image_repo
        self.image_factory = image_factory

    def get_task_repo(self, context):
        return self.task_repo

    def get_repo(self, context):
        return self.image_repo

    def get_db_task_repo(self, context):
        return self.task_repo

    def get_image_factory(self, context):
        return self.image_factory


class TestTaskImportExecutor(base.IsolatedUnitTest):

    def setUp(self):
        super(TestTaskImportExecutor, self).setUp()
        self.request = unit_utils.get_fake_request()
        self.context = self.request.context
        self.task_id = UUID1
        self.fake_task = unit_utils.FakeTask(self.task_id)
        self.fake_task_repo = FakeTaskRepo(self.fake_task)
        self.fake_image_repo = FakeImageRepo()
        self.fake_image_factory = FakeImageFactory(None)
        fake_gateway = FakeGateway(self.fake_task_repo,
                                   self.fake_image_repo,
                                   self.fake_image_factory)
        self.import_script = import_swift_to_swift.ImportScript(fake_gateway,
                                                                self.context)
        self.swift_store = import_swift_to_swift.SwiftStore()

    def test_unpack_task_input(self):
        fake_input_json = {
            "import_from": "blah",
            "import_from_format": "qcow2",
            "image_properties": {}
        }
        self.fake_task.input = fake_input_json
        data = self.import_script.unpack_task_input(self.fake_task)

        self.assertEquals(data, {
            'import_from': 'blah',
            'import_from_format': 'qcow2',
            'image_properties': {}
        })

    def test_unpack_task_input_missing_import_format(self):
        fake_input_json = {"input_from": "blah", "image_properties": {}}
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_script.unpack_task_input,
                          self.fake_task)

    def test_unpack_task_input_missing_import_from(self):
        fake_input_json = {
            "import_from_format": "qcow2",
            "image_properties": {}
        }
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_script.unpack_task_input,
                          self.fake_task)

    def test_unpack_task_input_missing_image_props(self):
        fake_input_json = {
            "import_from_format": "qcow2",
            "import_from": "blah"
        }
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_script.unpack_task_input,
                          self.fake_task)

    def test_unpack_task_input_invalid_json(self):
        fake_input_json = 'invalid'
        self.fake_task.input = fake_input_json
        self.assertRaises(exception.Invalid,
                          self.import_script.unpack_task_input,
                          self.fake_task)

    def test_create_image(self):
        fake_image = {
            "name": "test_name",
            "tags": ['tag1', 'tag2'],
            "foo": "bar"
        }
        with mock.patch.object(self.fake_image_factory,
                               'new_image') as mock_new_image:
            mock_new_image.return_value = {}
            with mock.patch.object(self.fake_image_repo, 'add') as mock_add:
                self.import_script.create_image(fake_image)

        mock_new_image.assert_called_once_with(extra_properties={'foo': 'bar'},
                                               tags=['tag1', 'tag2'],
                                               name='test_name')
        mock_add.assert_called_once_with({})

    #TODO(nikhil): to be fixed
    # def test_execute(self):
    #     with mock.patch.object(self.import_script, 'unpack_task_input') \
    #         as mock_unpack_task_input:
    #         mock_unpack_task_input.return_value = {'image_properties': {}}
    #         with mock.patch.object(self.import_script, 'create_image'):
    #             with mock.patch.object(self.import_script,
    #                                    'format_location_uri'):
    #                 with mock.patch.object(self.import_script,
    #                                        'set_image_data'):
    #                     self.import_script.execute(self.fake_task)
    #     self.assertEqual(self.fake_task.message, None)

    def test_execute_bad_input(self):
        with mock.patch.object(self.import_script,
                               'unpack_task_input') as unpack_mock:
            with mock.patch.object(self.fake_task_repo, 'save') as mock_save:
                unpack_mock.side_effect = exception.Invalid()
                self.import_script.execute(self.fake_task.task_id)

        unpack_mock.assert_called_once_with(self.fake_task)
        mock_save.assert_called_once_with(self.fake_task)
        self.assertNotEqual(self.fake_task.message, None)

    def test_invalid_input_from(self):
        location = 'invalid'
        self.assertRaises(exception.BadStoreUri,
                          self.import_script.format_location_uri,
                          location)

    def test_valid_input_from(self):
        container = str(uuid.uuid4())
        obj = str(uuid.uuid4())
        location = container + '/' + obj
        actual = self.import_script.format_location_uri(location)
        self.assertEqual((container, obj), actual)

    def test_set_image_data(self):
        fake_image = mock.Mock()
        data_file = mock.file_spec
        self.import_script.set_image_data(fake_image, data_file)
        fake_image.set_data.assert_called_once_with(mock.ANY)
