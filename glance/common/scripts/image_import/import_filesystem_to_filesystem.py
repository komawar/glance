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

from glance.common import exception
from glance.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class ImportScript(object):
    def __init__(self, gateway, context):
        self.gateway = gateway
        self.context = context

    def get_task(self, task_repo, task_id):
        task = None
        try:
            task = task_repo.get(task_id)
        except exception.NotFound as e:
            msg = _('Task not found for task_id {0}'.format(task_id))
            LOG.exception(msg, unicode(e))

        return task

    def import_image(self, task, uri):
        image = self.create_image(task.get('image_properties'))
        self.set_image_data(image, uri)
        image_repo = self.gateway.get_repo(self.context)
        image_id = image.image_id
        image = image_repo.get(image_id)
        image_repo.save(image)
        return image_id

    def execute(self, task_id):
        task_repo = self.gateway.get_task_repo(self.context)
        task = self.get_task(task_repo, task_id)

        if task is None:
            #TODO(nikhil): handle it in a better way
            return

        try:
            task_input = self.unpack_task_input(task)

            uri = self.format_location_uri(task_input.get('import_from', None))
            image_id = self.import_image(task_input, uri)

            task.success(result={'image_id': image_id})
        except Exception as e:
            #TODO(nikhil): need to bring back save_and_reraise_exception
            # with excutils.save_and_reraise_exception():
            err_msg = unicode(type(e)) + ': ' + unicode(e)
            LOG.exception(err_msg)

            task.fail(message={'error': err_msg})
        finally:
            task_repo.save(task)

    def unpack_task_input(self, task):
        task_input = task.input

        for key in ['import_from', 'import_from_format', 'image_properties']:
            if key not in task_input:
                msg = _("Input does not contain '%(key)s' field"
                        % {"key": key})
                raise exception.Invalid(msg)

        return task_input

    def format_location_uri(self, location):
        if not location or not location.startswith('file:///'):
            raise exception.BadStoreUri(_('Invalid location: %s') % location)

        return location[7:]

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
