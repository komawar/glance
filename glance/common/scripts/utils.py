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
import hashlib

import httplib
import time
import math

from oslo.config import cfg

from glance.common import exception
from glance.common import utils
from glance.openstack.common import excutils
from glance.openstack.common import log as logging
from glance.openstack.common import jsonutils
from glance.openstack.common import timeutils
import glance.store

try:
    import swiftclient
except ImportError:
    pass


ONE_MB = 1000 * 1024


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

CONF.import_opt('swift_store_auth_address', 'glance.store.swift')
CONF.import_opt('swift_store_auth_insecure', 'glance.store.swift')
CONF.import_opt('swift_store_auth_version', 'glance.store.swift')
CONF.import_opt('swift_store_region', 'glance.store.swift')
CONF.import_opt('swift_store_service_type', 'glance.store.swift')
CONF.import_opt('swift_store_endpoint_type', 'glance.store.swift')
CONF.import_opt('swift_enable_snet', 'glance.store.swift')
CONF.import_opt('swift_store_ssl_compression', 'glance.store.swift')
CONF.import_opt('swift_store_large_object_size', 'glance.store.swift')
CONF.import_opt('swift_store_large_object_chunk_size', 'glance.store.swift')
CONF.import_opt('admin_user', 'glance.registry.client')
CONF.import_opt('admin_password', 'glance.registry.client')
CONF.import_opt('admin_tenant_name', 'glance.registry.client')
CONF.import_opt('auth_url', 'glance.registry.client')


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
        self._obj_size = CONF.swift_store_large_object_size
        self.large_object_size = self._obj_size * ONE_MB
        self._chunk_size = CONF.swift_store_large_object_chunk_size
        self.large_object_chunk_size = self._chunk_size * ONE_MB

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
        response = None
        body = None
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

    def _delete_stale_chunks(self, connection, container, chunk_list):
            for chunk in chunk_list:
                LOG.debug(_("Deleting chunk %s") % chunk)
                try:
                    connection.delete_object(container, chunk)
                except Exception:
                    msg = _("Failed to delete orphaned chunk %s/%s")
                    LOG.exception(msg, container, chunk)

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

    def add(self, image_id, image_data, image_size,
            auth_token, container, connection=None):

        if not connection:
            connection = self.get_connection(auth_token)

        # Check if the container does not exists.
        try:
            connection.head_container(container)
        except swiftclient.ClientException as e:
            if e.http_status == 404:
                msg = _("container '%s' does not exist") % container
                LOG.exception(msg)
                raise exception.NotFound(message=msg)
            else:
                msg = _("Unexpected error while checking for container"
                        " '%s'") % container
                LOG.exception(msg)
                raise

        # Check if the object already exists in the container to avoid
        # over-writing.
        try:
            connection.head_object(container, image_id)
            msg = _("Swift already has an object with id '%(image_id)s' in "
                    "container '%(container)s'") % dict(image_id=image_id,
                                                        container=container)
            raise exception.Duplicate(msg)
        except swiftclient.ClientException as e:
            if e.http_status != 404:
                msg = _("Unexpected error while checking for object '%("
                        "image_id)s' in container "
                        "'%(container)s'") % dict(image_id=image_id,
                                                  container=container)
                LOG.exception(msg)
                raise

        image_data = utils.CooperativeReader(image_data)
        LOG.debug(_("Adding image object '%(obj_name)s' "
                    "to Swift") % dict(obj_name=image_id))
        try:
            if image_size > 0 and image_size < self.large_object_size:
                # Image size is known, and is less than large_object_size.
                # Send to Swift with regular PUT.
                obj_etag = connection.put_object(container,
                                                 image_id,
                                                 image_data,
                                                 content_length=image_size)
            else:
                # Write the image into Swift in chunks.
                chunk_id = 1
                if image_size > 0:
                    total_chunks = str(int(
                        math.ceil(float(image_size) /
                                  float(self.large_object_chunk_size))))
                else:
                    # image_size == 0 is when we don't know the size
                    # of the image. This can occur with older clients
                    # that don't inspect the payload size.
                    LOG.debug(_("Cannot determine image size. Adding as a "
                                "segmented object to Swift."))
                    total_chunks = '?'

                checksum = hashlib.md5()
                written_chunks = []
                combined_chunks_size = 0
                while True:
                    chunk_size = self.large_object_chunk_size
                    if image_size == 0:
                        content_length = None
                    else:
                        left = image_size - combined_chunks_size
                        if left == 0:
                            break
                        if chunk_size > left:
                            chunk_size = left
                        content_length = chunk_size

                    chunk_name = "%s-%05d" % (image_id, chunk_id)
                    reader = ChunkReader(image_data, checksum, chunk_size)
                    chunk_etag = None
                    try:
                        chunk_etag = connection.put_object(
                            container, chunk_name,
                            reader, content_length=content_length)
                        written_chunks.append(chunk_name)
                    except Exception:
                        # Delete orphaned segments from swift backend
                        with excutils.save_and_reraise_exception():
                            LOG.exception(_("Error during chunked upload to "
                                            "backend, deleting stale chunks"))
                            self._delete_stale_chunks(connection,
                                                      container,
                                                      written_chunks)

                    bytes_read = reader.bytes_read
                    msg = (_("Wrote chunk %(chunk_name)s (%(chunk_id)d/"
                             "%(total_chunks)s) of length %(bytes_read)d "
                             "to Swift returning MD5 of content: "
                             "%(chunk_etag)s") %
                           {'chunk_name': chunk_name,
                            'chunk_id': chunk_id,
                            'total_chunks': total_chunks,
                            'bytes_read': bytes_read,
                            'chunk_etag': chunk_etag})
                    LOG.debug(msg)

                    if bytes_read == 0:
                        # Delete the last chunk, because it's of zero size.
                        # This will happen if size == 0.
                        LOG.debug(_("Deleting final zero-length chunk"))
                        connection.delete_object(container,
                                                 chunk_name)
                        break

                    chunk_id += 1
                    combined_chunks_size += bytes_read

                # In the case we have been given an unknown image size,
                # set the size to the total size of the combined chunks.
                if image_size == 0:
                    image_size = combined_chunks_size

                # Now we write the object manifest and return the
                # manifest's etag...
                manifest = "%s/%s-" % (container, image_id)
                headers = {'ETag': hashlib.md5("").hexdigest(),
                           'X-Object-Manifest': manifest}

                # The ETag returned for the manifest is actually the
                # MD5 hash of the concatenated checksums of the strings
                # of each chunk...so we ignore this result in favour of
                # the MD5 of the entire image file contents, so that
                # users can verify the image file contents accordingly
                connection.put_object(container, image_id,
                                      None,
                                      headers=headers)
                obj_etag = checksum.hexdigest()

            # NOTE: We return the user and key here! Have to because
            # location is used by the API server to return the actual
            # image data. We *really* should consider NOT returning
            # the location attribute from GET /images/<ID> and
            # GET /images/details

            # return (location.get_uri(), image_size, obj_etag, {})
            return obj_etag

        except swiftclient.ClientException as e:
            if e.http_status == httplib.CONFLICT:
                raise exception.Duplicate(_("Swift already has an image at "
                                            "this location"))
            msg = (_("Failed to add object to Swift.\n"
                     "Got error from Swift: %(e)s") % {'e': e})
            LOG.error(msg)
            raise glance.store.BackendException(msg)


class ChunkReader(object):
    def __init__(self, fd, checksum, total):
        self.fd = fd
        self.checksum = checksum
        self.total = total
        self.bytes_read = 0

    def read(self, i):
        left = self.total - self.bytes_read
        if i > left:
            i = left
        result = self.fd.read(i)
        self.bytes_read += len(result)
        self.checksum.update(result)
        return result
