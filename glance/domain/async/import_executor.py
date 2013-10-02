# Copyright 2013 OpenStack Foundation
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

import eventlet

from glance.common import exception
from glance.domain import async
from glance.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class TaskImportExecutor(async.TaskExecutorInterface):

    def execute(self, task):
        #TODO unpack json input in task, catch exceptions and set task message
        task_repo = self.gateway.get_db_task_repo(self.context)
        try:
            input = self.unpack_task_input(task)
            image = self.create_image(input.get('image_properties'))
            uri = self.check_valid_location(input)

            self.set_image_data(image, uri)
            image_repo = self.gateway.get_repo(self.context)
            image_id = image.image_id
            image = image_repo.get(image_id)
            image_repo.save(image)

            task.result = image_id
            task_repo.save(task)
        except Exception as e:
            task.message = unicode(type(e)) + ': ' + unicode(e)
            LOG.exception("")
            task._status = 'failure'
            task_repo.save(task)

    def run(self, task_proxy):
        msg = _("Running task '%(task_id)s'") % {'task_id': task_proxy.task_id}
        LOG.info(msg)
        eventlet.spawn_n(self.execute, task_proxy)
        self.set_task_status(task_proxy)

    def unpack_task_input(self, task):
        task_input = task.input

        for key in ["import_from", "import_from_format", "image_properties"]:
            if not key in task_input:
                msg = _("Input does not contain '%(key)s' field"
                        % {"key": key})
                raise exception.Invalid(msg)

        return task_input

    def create_image(self, image_properties):
        _base_properties = ['checksum', 'created_at', 'container_format',
                            'disk_format', 'id', 'min_disk', 'min_ram',
                            'name', 'size', 'status', 'tags', 'updated_at',
                            'visibility', 'protected']
        image_factory = self.gateway.get_image_factory(self.context)
        image_repo = self.gateway.get_repo(self.context)

        image = {}
        properties = image_properties
        tags = properties.pop('tags', None)
        for key in _base_properties:
            try:
                image[key] = properties.pop(key)
            except KeyError:
                pass
        image.pop('image_id', None)
        image = image_factory.new_image(tags=tags, extra_properties=properties,
                                        **image)
        image_repo.add(image)
        return image

    def check_valid_location(self, input):
        uri = input.get('import_from', None)
        if not uri or not uri.startswith('file:///'):
            raise exception.BadStoreUri(_('Invalid location: %s') % uri)

        return uri[7:]

    def set_image_data(self, image, uri):
        try:
            with open(uri, 'r') as image_data:
                LOG.info(_("Got image file %(image_file)s to be imported") %
                         {"image_file": uri})
                image.set_data(image_data)
        except Exception as e:
            LOG.warn(_("Task failed with exception %(task_error)s") %
                     {"task_error": str(e)})
            LOG.info(_("Could not import image file %(image_file)s") %
                     {"image_file": uri})
            raise e

    def set_task_status(self, task):
        task._status = 'success'
        task_repo = self.gateway.get_db_task_repo(self.context)
        task_repo.save(task)
