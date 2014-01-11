# -*- coding: utf-8 -*-
#
# Copyright 2013 OpenStack LLC.
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
import testtools

from glance.common import exception
from glance.common.scripts.image_export import export_swift_to_swift


class ExportScriptTestCase(testtools.TestCase):
    def setUp(self):
        super(ExportScriptTestCase, self).setUp()
        self.mock_gateway = mock.Mock()
        self.mock_task = mock.Mock()
        self.mock_task.task_id = 'blah'
        self.mock_image_id = mock.Mock()
        self.mock_task_repo = mock.Mock()
        self.mock_task_repo.get.return_value = self.mock_task
        self.mock_gateway.get_task_repo.return_value = self.mock_task_repo
        self.mock_context = mock.Mock()
        self.export_script = export_swift_to_swift.ExportScript(
            gateway=self.mock_gateway,
            context=self.mock_context)

    def test_execute(self):
        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": 'blah foo',
        }
        self.export_script.transfer_image_data = mock.Mock()

        self.export_script.execute(task_id='blah')

        fake_task_repo = self.mock_gateway.get_task_repo
        fake_task_repo.assert_called_once_with(self.mock_context)
        self.mock_task.begin_processing.assert_called_once_with()
        self.mock_task_repo.save.assert_called_once_with(self.mock_task)

    def test_execute_task_not_found(self):
        self.mock_task_repo.get.return_value = None

        self.export_script.execute(task_id='blah')

        #NOTE(ameade): Execution should fail silently

    def test_execute_image_not_found(self):
        self.export_script.transfer_image_data = mock.Mock()
        fake_image_uuid = str(uuid.uuid4())
        fake_container = 'blah'
        self.mock_task.input = {
            "image_uuid": fake_image_uuid,
            "receiving_swift_container": fake_container,
        }
        self.export_script.execute(task_id='blah')
        self.mock_gateway.get_task_repo.assert_called_once_with(
            self.mock_context)
        self.export_script.transfer_image_data.assert_called_once_with(
            fake_image_uuid, fake_container)

        #NOTE(ameade): Execution should fail silently

    def test_get_task(self):
        task = self.export_script.get_task(self.mock_task_repo, 'fake_task_id')
        self.assertEqual(task, self.mock_task)

    def test_get_task_not_found(self):
        self.mock_task_repo.get.side_effect = exception.NotFound
        task = self.export_script.get_task(self.mock_task_repo, 'fake_task_id')
        self.assertEqual(task, None)

    def test_validate_task_input(self):
        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": 'blah foo',
        }
        self.export_script.validate_task_input(self.mock_task)

    def test_validate_task_input_with_unicode(self):
        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": "ಠ_ಠ",
        }
        self.export_script.validate_task_input(self.mock_task)

    def test_validate_task_input_invalid_input(self):
        self.mock_task.input = {}

        self.assertRaises(exception.Invalid,
                          self.export_script.validate_task_input,
                          self.mock_task)

    def test_validate_task_input_invalid_image_uuid(self):
        self.mock_task.input = {
            "image_uuid": 'not a uuid',
            "receiving_swift_container": ''
        }

        self.assertRaises(exception.Invalid,
                          self.export_script.validate_task_input,
                          self.mock_task)

    def test_validate_task_input_invalid_receiving_swift_container(self):
        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": ''
        }

        self.assertRaises(exception.Invalid,
                          self.export_script.validate_task_input,
                          self.mock_task)

    def test_validate_task_input_receiving_swift_container_special_chars(self):
        """Ensure a container name can not contain a '?', '.', or '/' in the
        name.
        """
        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": '?'
        }

        self.assertRaises(exception.Invalid,
                          self.export_script.validate_task_input,
                          self.mock_task)

        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": '/'
        }

        self.assertRaises(exception.Invalid,
                          self.export_script.validate_task_input,
                          self.mock_task)

        self.mock_task.input = {
            "image_uuid": str(uuid.uuid4()),
            "receiving_swift_container": '.'
        }

        self.assertRaises(exception.Invalid,
                          self.export_script.validate_task_input,
                          self.mock_task)

    def test_transfer_image_data_image_not_found(self):
        fake_image_id = uuid.uuid4()
        fake_container = 'blah'
        self.export_script.image_repo.get = mock.Mock()
        self.export_script.image_repo.get.side_effect = exception.NotFound

        self.assertRaises(exception.NotFound,
                          self.export_script.transfer_image_data,
                          fake_image_id,
                          fake_container)

    def test_transfer_image_data_get_image_data(self):
        fake_image = mock.Mock()
        fake_container = 'blah'
        self.export_script.image_repo.get.return_value = fake_image
        self.export_script.upload_image_data = mock.Mock()
        self.export_script.upload_image_data.return_value = None

        self.export_script.transfer_image_data(fake_image, fake_container)

        fake_image.get_data.assert_called_once_with()

    def test_transfer_image_data_object_exists(self):
        fake_data_iter = 'foo'
        fake_container = 'bar'
        self.export_script.upload_image_data = mock.Mock()
        self.export_script.image_repo.get = mock.Mock()
        fake_image = mock.Mock()
        fake_image.get_data.return_value = None
        self.export_script.upload_image_data.side_effect = exception.Duplicate

        self.assertRaises(exception.Duplicate,
                          self.export_script.transfer_image_data,
                          fake_image,
                          'fake_container')

    def test_upload_image_data_object_exists(self):
        fake_image_data_iter = mock.Mock()
        fake_image_id = mock.Mock()

        fake_swift_store_cls = 'glance.common.scripts.utils.SwiftStore'
        with mock.patch(fake_swift_store_cls) as mock_swift_store:
            mock_swift_store.get.return_value = ('data_iter', 'length')
            self.assertRaises(exception.Duplicate,
                              self.export_script.upload_image_data,
                              fake_image_id,
                              fake_image_data_iter,
                              'fake_container')
