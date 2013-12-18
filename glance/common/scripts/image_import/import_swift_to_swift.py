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


import httplib

from oslo.config import cfg

from glance.common import exception
from glance.openstack.common import log as logging
from glance.openstack.common import uuidutils
import glance.store

try:
    import swiftclient
except ImportError:
    pass


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


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

    def import_image(self, task, container, obj):
        image = self.create_image(task.get('image_properties'))
        image_data_file = self._get_image_data(container, obj)
        self.set_image_data(image, image_data_file)
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

            input_loc = task_input.get('import_from', None)
            container, obj = self.format_location_uri(input_loc)
            image_id = self.import_image(task_input, container, obj)

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
        try:
            container = location[0:36]
            obj = location[37:]
        except IndexError:
            raise exception.BadStoreUri(_('Invalid location: %s') % location)

        if (not location or not uuidutils.is_uuid_like(container)
           or location[36]is not '/' or not uuidutils.is_uuid_like(obj)):

            raise exception.BadStoreUri(_('Invalid location: %s') % location)

        return container, obj

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

    def _get_image_data(self, container, object):
        image_data_file, length = SwiftStore.get(container, object)
        return image_data_file

    def set_image_data(self, image, data_file):
        try:
            image_data = data_file
            # with open(data_file, 'r') as image_data:
            #     LOG.info(_("Got image file %(image_file)s to be imported") %
            #              {"image_file": data_file})
            image.set_data(image_data)
        except Exception as e:
            LOG.warn(_("Task failed with exception %(task_error)s") %
                     {"task_error": str(e)})
            LOG.info(_("Could not import image file %(image_file)s") %
                     {"image_file": data_file})
            raise e


class SwiftStore(object):
    CHUNKSIZE = 65536  # NOTE(nikhil): Same as swift BaseStore

    def __init__(self):
        self.user = CONF.swift_store_user
        self.key = CONF.swift_store_key
        self.auth_url = CONF.swift_store_auth_address
        self.insecure = CONF.swift_store_auth_insecure
        self.auth_version = CONF.swift_store_auth_version
        self.region = CONF.swift_store_region
        self.service_type = CONF.swift_store_service_type
        self.endpoint_type = CONF.swift_store_endpoint_type
        self.snet = CONF.swift_enable_snet
        self.ssl_compression = CONF.swift_store_ssl_compression

    def _option_get(self, param):
        result = getattr(CONF, param)
        if not result:
            reason = (_("Could not find %(param)s in configuration "
                        "options.") % {'param': param})
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="swift",
                                                  reason=reason)
        return result

    def get_connection(self):
        auth_url = self.auth_url
        if not auth_url.endswith('/'):
            auth_url += '/'

        os_options = {}
        if self.region:
            os_options['region_name'] = self.region
        os_options['endpoint_type'] = self.endpoint_type
        os_options['service_type'] = self.service_type

        return swiftclient.Connection(auth_url,
                                      self.user,
                                      self.key,
                                      insecure=self.insecure,
                                      snet=self.snet,
                                      auth_version=self.auth_version,
                                      os_options=os_options,
                                      ssl_compression=self.ssl_compression)

    def get(self, container, obj, connection=None):
        if not connection:
            connection = self.get_connection()

        try:
            resp_headers, resp_body = connection.get_object(
                container=container,
                obj=obj,
                resp_chunk_size=self.CHUNKSIZE)
        except swiftclient.ClientException as e:
            if e.http_status == httplib.NOT_FOUND:
                msg = _("Swift could not find object %s." % obj)
                LOG.warn(msg)
                raise exception.NotFound(msg)
            else:
                raise

        class ResponseIndexable(glance.store.Indexable):
            def another(self):
                try:
                    return self.wrapped.next()
                except StopIteration:
                    return ''

        length = int(resp_headers.get('content-length', 0))
        return ResponseIndexable(resp_body, length), length
