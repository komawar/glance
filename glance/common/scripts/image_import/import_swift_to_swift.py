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
import time

from oslo.config import cfg

from glance.common import exception
from glance.openstack.common import log as logging
from glance.openstack.common import jsonutils
from glance.openstack.common import timeutils
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
        self.image_repo = self.gateway.get_repo(self.context)

    def get_task(self, task_repo, task_id):
        task = None
        try:
            task = task_repo.get(task_id)
        except exception.NotFound as e:
            msg = _('Task not found for task_id %s') % task_id
            LOG.exception(msg, unicode(e))

        return task

    def _set_base_image_properties(self, image):
        image.container_format = 'ovf'
        image.disk_format = 'vhd'
        self.image_repo.save(image)  # NOTE(nikhil): changed

    def _set_image_in_db_after_upload(self, image):
        # NOTE(nikhil): There were issues setting the image in DB  using the
        # same ImageProxy object after upload of image data. So, we need to set
        # the necessary properties on a new ImageProxy object.
        new_image = self.image_repo.get(image.image_id)
        # NOTE(nikhil): copying over the properties which could not be set
        new_image.size = image.size
        new_image.checksum = image.checksum
        new_image.status = image.status
        new_image.locations.append(dict(image.locations[0]))
        self.image_repo.save(new_image)

    def import_image(self, task, container, obj):
        try:
            image_data_file = self._get_image_data(container, obj)
            image = self.create_image(task.get('image_properties'))

            self._set_base_image_properties(image)
            self.set_image_data(image, image_data_file)

            self._set_image_in_db_after_upload(image)

        except Exception as e:
            LOG.exception(_('Failed to save image while trying to import'))
            raise e

        return image.image_id

    def execute(self, task_id):
        task_repo = self.gateway.get_task_repo(self.context)
        task = self.get_task(task_repo, task_id)

        if task is None:
            #TODO(nikhil): handle it in a better way
            LOG.error(_("Task corresponding to task_id %s was not found "
                        "while attempting to execute it.") % task_id)
            return
        else:
            LOG.info(_("Executor is beginning processing on "
                       "Task %s") % task_id)
            task.begin_processing()
            task_repo.save(task)

        try:
            task_input = self.unpack_task_input(task)

            input_loc = task_input.get('import_from', None)
            container, obj = self.format_location_uri(input_loc)
            image_id = self.import_image(task_input, container, obj)

            task.succeed(result={'image_id': image_id})
        except Exception as e:
            #TODO(nikhil): need to bring back save_and_reraise_exception
            # with excutils.save_and_reraise_exception():

            err_msg = unicode(type(e))  # NOTE(nikhil): changed
            LOG.exception(err_msg)

            task.fail(message='Error: %s' % unicode(e))
        finally:
            LOG.warn(_("Setting task status to %s.") % task.status)
            try:
                task_repo.save(task)
            except Exception as e:
                LOG.exception(_("Task execution finished, yet failed to save "
                                "the task %s in the database.") % task.task_id)

    def unpack_task_input(self, task):
        task_input = task.input

        for key in ['import_from', 'import_from_format', 'image_properties']:
            if key not in task_input:
                msg = (_("Input does not contain '%(key)s' "
                         "field") % {"key": key})
                raise exception.Invalid(msg)

        return task_input

    def format_location_uri(self, location):
        try:
            container = location.split("/")[0]
            obj = location.split("/")[1]
        except IndexError:
            raise exception.BadStoreUri(_('Invalid location: %s') % location)

        if not location:

            raise exception.BadStoreUri(_('Invalid location: %s') % location)

        return container, obj

    def create_image(self, image_properties):
        _base_properties = ['checksum', 'created_at', 'container_format',
                            'disk_format', 'id', 'min_disk', 'min_ram',
                            'name', 'size', 'status', 'tags', 'updated_at',
                            'visibility', 'protected']
        image_factory = self.gateway.get_image_factory(self.context)

        image = {}
        properties = image_properties
        tags = properties.pop('tags', None)
        for key in _base_properties:
            try:
                image[key] = properties.pop(key)
            except KeyError:
                pass

        extra_properties = {
            "com.rackspace__1__build_core": "1",
            "com.rackspace__1__build_managed": "1",
            "com.rackspace__1__visible_core": "1",
            "com.rackspace__1__visible_managed": "1",
            "com.rackspace__1__options": "0",
            "com.rackspace__1__build_rackconnect": "1",
            "com.rackspace__1__visible_rackconnect": "1",
            "org.openstack__1__architecture": "x64",
        }
        properties.update(extra_properties)

        image.pop('image_id', None)
        try:
            image = image_factory.new_image(tags=tags,
                                            extra_properties=properties,
                                            **image)
        except Exception as e:
            raise e
        self.image_repo.add(image)
        return image

    def _get_image_data(self, container, object):
        auth_token = getattr(self.context, 'auth_tok', None)
        LOG.info(_("Attempting to get Image data object %(obj)s from user's "
                   "container %(container)s") %
                 {'obj': object, 'container': container})
        store = SwiftStore()
        image_data_file, length = store.get(container, object, auth_token)
        LOG.info(_("Data object %(obj)s obtained from user's container "
                   "%(container)s") % {'obj': object, 'container': container})
        return image_data_file

    def set_image_data(self, image, data_file):
        try:
            image_data = data_file
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

    def _get_http_connection(self):
        auth_url = CONF.auth_url
        auth_host = auth_url.split('//')[1]
        auth_host = auth_host.split('/')[0]
        auth_port = '443'
        LOG.info(_("Attempting to get HTTP conn on auth_host %(auth_host)s, "
                   "auth_port %(auth_port)s") % {'auth_host': auth_host,
                                                 'auth_port': auth_port})
        return httplib.HTTPSConnection(auth_host,
                                       auth_port)

    def _http_request(self, method, path, **kwargs):
        """HTTP request helper used to make unspecified content type requests.

        :param method: http method
        :param path: relative request url
        :return (http response object, response body)
        :raise ServerError when unable to communicate with keystone

        """
        conn = self._get_http_connection()

        RETRIES = 5
        retry = 0
        while True:
            try:
                conn.request(method, path, **kwargs)
                response = conn.getresponse()
                body = response.read()
                break
            except Exception as e:
                if retry == RETRIES:
                    LOG.error('HTTP connection exception: %s' % e)
                    raise
                # NOTE(nikhil): sleep 0.5, 1, 2
                LOG.warn(_('Retrying on HTTP connection exception: %s') % e)
                time.sleep(2.0 ** retry / 2)
                retry += 1
            finally:
                conn.close()

        return response, body

    def _json_request(self, method, path, body=None, additional_headers=None):
        """HTTP request helper used to make json requests.

        :param method: http method
        :param path: relative request url
        :param body: dict to encode to json as request body. Optional.
        :param additional_headers: dict of additional headers to send with
                                   http request. Optional.
        :return (http response object, response body parsed as json)

        """
        kwargs = {
            'headers': {
                'Content-type': 'application/json',
                'Accept': 'application/json',
            },
        }

        if additional_headers:
            kwargs['headers'].update(additional_headers)

        if body:
            kwargs['body'] = jsonutils.dumps(body)

        response, body = self._http_request(method, path, **kwargs)

        try:
            data = jsonutils.loads(body)
        except ValueError:
            LOG.debug(_('Keystone did not return json-encoded body'))
            data = {}
            raise  # NOTE(nikhil): added

        return response, data

    def _request_admin_token(self):
        """Retrieve new token as admin user from keystone.

        :return token id upon success

        Irrespective of the auth version we are going to use for the
        user token, for simplicity we always use a v2 admin token to
        validate the user token.

        """
        params = {
            'auth': {
                'passwordCredentials': {
                    'username': CONF.admin_user,
                    'password': CONF.admin_password,
                },
                'tenantName': CONF.admin_tenant_name,
            }
        }

        response, data = self._json_request('POST',
                                            '/v2.0/tokens',
                                            body=params)
        try:
            token = data['access']['token']['id']
            expiry = data['access']['token']['expires']
            if not (token and expiry):
                raise AssertionError('invalid token or expire')
            datetime_expiry = timeutils.parse_isotime(expiry)
            return token, timeutils.normalize_time(datetime_expiry)
        except (AssertionError, KeyError):
            LOG.warn(_("Unexpected response from keystone service: %s") % data)
            raise  # NOTE(nikhil): changed to raise wildcard exception
        except ValueError:
            LOG.warn(_("Unable to parse expiration time from token: "
                       "%s") % data)
            raise  # NOTE(nikhil): changed to raise wildcard exception

    def get_connection(self, auth_token):
        admin_token = self._request_admin_token()[0]
        headers = {'X-Auth-Token': admin_token}
        path = '/v2.0/tokens/%s/endpoints' % auth_token  # NOTE(nikhil): added
        resp, data = self._json_request('GET',
                                        path,
                                        additional_headers=headers)

        auth_url = None
        for endpoint in data.get('endpoints'):
            if endpoint:
                region = endpoint.get('region')
                service_type = endpoint.get('type')
                service_name = endpoint.get('name')
                if (service_type and service_type == 'object-store'):
                    if (service_name and service_name == 'cloudFiles'):
                        if (region and region == self.region):
                            auth_url = endpoint['publicURL']
                            break

        os_options = {}
        if self.region:
            os_options['region_name'] = self.region

        os_options['endpoint_type'] = self.endpoint_type
        os_options['service_type'] = self.service_type

        return swiftclient.Connection(preauthurl=auth_url,
                                      preauthtoken=auth_token,
                                      insecure=self.insecure,
                                      snet=self.snet,
                                      auth_version=self.auth_version,
                                      os_options=os_options,
                                      ssl_compression=self.ssl_compression)

    def get(self, container, obj, auth_token, connection=None):
        if not connection:
            try:  # NOTE(nikhil): added the try block
                connection = self.get_connection(auth_token)
            except Exception:
                LOG.exception(_("Could not establish connection to swift "
                                "using swiftclient"))
                raise

        try:
            msg = _("Attempting to download object %s.") % obj
            LOG.info(msg)
            resp_headers, resp_body = connection.get_object(
                container=container,
                obj=obj,
                resp_chunk_size=self.CHUNKSIZE)
        except swiftclient.ClientException as e:
            if e.http_status == httplib.NOT_FOUND:
                msg = _("Swift could not find object %s.") % obj
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
        if length != 0:
            return ResponseIndexable(resp_body, length), length
        else:
            LOG.warn(_("_No object returned."))
