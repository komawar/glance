# Copyright 2012 OpenStack LLC.
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

import datetime
import json
import stubout

import webob

import glance.api.v2.images
from glance.common import exception
from glance.common import utils
import glance.schema
import glance.tests.unit.utils as unit_test_utils
import glance.tests.utils as test_utils
from glance.openstack.common import cfg


DATETIME = datetime.datetime(2012, 5, 16, 15, 27, 36, 325355)
ISOTIME = '2012-05-16T15:27:36Z'


CONF = cfg.CONF


class TestImagesController(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesController, self).setUp()
        self.db = unit_test_utils.FakeDB()
        self.controller = glance.api.v2.images.ImagesController(self.db)
        self.stubs = stubout.StubOutForTesting()

    def tearDown(self):
        super(TestImagesController, self).tearDown()
        self.stubs.UnsetAll()

    def test_index(self):
        request = unit_test_utils.get_fake_request()
        output = self.controller.index(request)
        self.assertEqual(2, len(output))
        self.assertEqual(output[0]['id'], unit_test_utils.UUID1)
        self.assertEqual(output[1]['id'], unit_test_utils.UUID2)

    def test_index_with_marker(self):
        called = {'image_get_all': False}

        def fake_images_get_all(*_args, **_kwargs):
            self.assertEqual(_kwargs.get('marker'), unit_test_utils.UUID1)
            called['image_get_all'] = True
            return []
        self.stubs.Set(self.db, 'image_get_all', fake_images_get_all)

        path = '/images?marker=%s' % unit_test_utils.UUID1
        request = unit_test_utils.get_fake_request(path)
        output = self.controller.index(request, marker=unit_test_utils.UUID1)
        self.assertTrue(called['image_get_all'])

    def test_index_with_limit(self):
        called = {'image_get_all': False}
        limit = 1

        def fake_images_get_all(*_args, **_kwargs):
            self.assertEqual(_kwargs.get('limit'), limit)
            called['image_get_all'] = True
            return []
        self.stubs.Set(self.db, 'image_get_all', fake_images_get_all)

        path = '/images?limit=%s' % limit
        request = unit_test_utils.get_fake_request(path)
        output = self.controller.index(request, limit=limit)
        self.assertTrue(called['image_get_all'])

    def test_index_with_sort_dir(self):
        called = {'image_get_all': False}
        sort_dir = 'asc'

        def fake_images_get_all(*_args, **_kwargs):
            self.assertEqual(_kwargs.get('sort_dir'), sort_dir)
            called['image_get_all'] = True
            return []
        self.stubs.Set(self.db, 'image_get_all', fake_images_get_all)

        path = '/images?sort_dir=%s' % sort_dir
        request = unit_test_utils.get_fake_request(path)
        output = self.controller.index(request, sort_dir=sort_dir)
        self.assertTrue(called['image_get_all'])

    def test_index_with_sort_key(self):
        called = {'image_get_all': False}
        sort_key = 'id'

        def fake_images_get_all(*_args, **_kwargs):
            self.assertEqual(_kwargs.get('sort_key'), sort_key)
            called['image_get_all'] = True
            return []
        self.stubs.Set(self.db, 'image_get_all', fake_images_get_all)

        path = '/images?sort_key=%s' % sort_key
        request = unit_test_utils.get_fake_request(path)
        output = self.controller.index(request, sort_key=sort_key)
        self.assertTrue(called['image_get_all'])

    def test_index_invalid_sort_key(self):

        def fake_raises_invalid_sort_key(*_args, **_kwargs):
            raise exception.InvalidSortKey()
        self.stubs.Set(self.db, 'image_get_all', fake_raises_invalid_sort_key)

        request = unit_test_utils.get_fake_request('/images?sort_key=blah')
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.index,
                          request)

    def test_index_with_marker_not_found(self):
        fake_uuid = utils.generate_uuid()
        request = unit_test_utils.get_fake_request()

        def raise_not_found(*_args, **_kwargs):
            raise exception.NotFound()
        self.stubs.Set(self.db, "image_get_all", raise_not_found)
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.index, request, marker=fake_uuid)

    def test_index_zero_images(self):
        self.db.reset()
        request = unit_test_utils.get_fake_request()
        output = self.controller.index(request)
        self.assertEqual([], output)

    def test_show(self):
        request = unit_test_utils.get_fake_request()
        output = self.controller.show(request, image_id=unit_test_utils.UUID2)
        for key in ['created_at', 'updated_at']:
            output.pop(key)
        expected = {
            'id': unit_test_utils.UUID2,
            'name': 'image-name',
            'owner': unit_test_utils.TENANT1,
            'location': None,
            'status': 'queued',
            'is_public': False,
            'tags': [],
            'members': [],
            'properties': {},
        }
        self.assertEqual(expected, output)

    def test_show_non_existant(self):
        request = unit_test_utils.get_fake_request()
        image_id = utils.generate_uuid()
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.show, request, image_id)

    def test_create(self):
        request = unit_test_utils.get_fake_request()
        image = {'name': 'image-1'}
        output = self.controller.create(request, image)
        for key in ['id', 'created_at', 'updated_at']:
            output.pop(key)
        expected = {
            'name': 'image-1',
            'owner': unit_test_utils.TENANT1,
            'location': None,
            'status': 'queued',
            'is_public': False,
            'properties': {},
            'tags': [],
        }
        self.assertEqual(expected, output)

    def test_create_with_owner_forbidden(self):
        request = unit_test_utils.get_fake_request()
        image = {'name': 'image-1', 'owner': utils.generate_uuid()}
        self.assertRaises(webob.exc.HTTPForbidden, self.controller.create,
                          request, image)

    def test_create_public_image_as_admin(self):
        request = unit_test_utils.get_fake_request()
        image = {'name': 'image-1', 'is_public': True}
        output = self.controller.create(request, image)
        for key in ['id', 'created_at', 'updated_at']:
            output.pop(key)
        expected = {
            'name': 'image-1',
            'owner': unit_test_utils.TENANT1,
            'location': None,
            'status': 'queued',
            'is_public': True,
            'tags': [],
            'properties': {},
        }
        self.assertEqual(expected, output)

    def test_update(self):
        request = unit_test_utils.get_fake_request()
        image = {'name': 'image-2'}
        output = self.controller.update(request, unit_test_utils.UUID1, image)
        for key in ['id', 'created_at', 'updated_at']:
            output.pop(key)
        expected = {
            'name': 'image-2',
            'owner': unit_test_utils.TENANT1,
            'location': unit_test_utils.UUID1,
            'status': 'queued',
            'is_public': False,
            'tags': ['ping', 'pong'],
            'properties': {},
        }
        self.assertEqual(expected, output)

    def test_update_non_existant(self):
        request = unit_test_utils.get_fake_request()
        image = {'name': 'image-2'}
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, utils.generate_uuid(), image)

    def test_index_with_invalid_marker(self):
        fake_uuid = utils.generate_uuid()
        request = unit_test_utils.get_fake_request()

        def raise_not_found(*_args, **_kwargs):
            raise exception.NotFound()
        self.stubs.Set(self.db, "image_get_all", raise_not_found)
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.index, request, marker=fake_uuid)


class TestImagesDeserializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesDeserializer, self).setUp()
        schema_api = glance.schema.API()
        self.deserializer = glance.api.v2.images.RequestDeserializer(
            schema_api)

    def test_create_with_id(self):
        request = unit_test_utils.get_fake_request()
        image_id = utils.generate_uuid()
        request.body = json.dumps({'id': image_id})
        output = self.deserializer.create(request)
        expected = {'image': {'id': image_id, 'properties': {}}}
        self.assertEqual(expected, output)

    def test_create_with_name(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'name': 'image-1'})
        output = self.deserializer.create(request)
        expected = {'image': {'name': 'image-1', 'properties': {}}}
        self.assertEqual(expected, output)

    def test_create_public(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'visibility': 'public'})
        output = self.deserializer.create(request)
        expected = {'image': {'is_public': True, 'properties': {}}}
        self.assertEqual(expected, output)

    def test_create_private(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'visibility': 'private'})
        output = self.deserializer.create(request)
        expected = {'image': {'is_public': False, 'properties': {}}}
        self.assertEqual(expected, output)

    def test_create_readonly_attributes_ignored(self):
        for key in ['created_at', 'updated_at']:
            request = unit_test_utils.get_fake_request()
            request.body = json.dumps({key: ISOTIME})
            output = self.deserializer.create(request)
            expected = {'image': {'properties': {}}}
            self.assertEqual(expected, output)

    def test_create_with_tags(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'tags': ['one', 'two']})
        output = self.deserializer.create(request)
        expected = {'image': {'tags': ['one', 'two'], 'properties': {}}}
        self.assertEqual(expected, output)

    def test_update(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'name': 'image-1', 'visibility': 'public'})
        output = self.deserializer.update(request)
        expected = {
            'image': {
                'name': 'image-1',
                'is_public': True,
                'properties': {},
            },
        }
        self.assertEqual(expected, output)

    def test_update_readonly_attributes_ignored(self):
        for key in ['created_at', 'updated_at']:
            request = unit_test_utils.get_fake_request()
            request.body = json.dumps({key: ISOTIME})
            output = self.deserializer.update(request)
            expected = {'image': {'properties': {}}}
            self.assertEqual(expected, output)

    def test_index(self):
        marker = utils.generate_uuid()
        path = '/images?limit=1&marker=%s' % marker
        request = unit_test_utils.get_fake_request(path)
        expected = {'limit': 1,
                    'marker': marker,
                    'sort_key': 'created_at',
                    'sort_dir': 'desc'}
        output = self.deserializer.index(request)
        self.assertEqual(output, expected)

    def test_index_default(self):
        limit_param_default = CONF.limit_param_default
        request = unit_test_utils.get_fake_request('/images')
        expected = {'limit': limit_param_default,
                    'sort_key': 'created_at',
                    'sort_dir': 'desc'}
        output = self.deserializer.index(request)
        self.assertEqual(output, expected)

    def test_index_non_integer_limit(self):
        request = unit_test_utils.get_fake_request('/images?limit=blah')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)

    def test_index_zero_limit(self):
        request = unit_test_utils.get_fake_request('/images?limit=0')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)

    def test_index_negative_limit(self):
        request = unit_test_utils.get_fake_request('/images?limit=-1')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)

    def test_index_fraction(self):
        request = unit_test_utils.get_fake_request('/images?limit=1.1')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)

    def test_index_greater_than_limit_max(self):
        api_limit_max = CONF.api_limit_max
        url = '/images?limit=%s' % (api_limit_max + 1)
        request = unit_test_utils.get_fake_request(url)
        output = self.deserializer.index(request)
        self.assertEqual(output['limit'], api_limit_max)

    def test_index_marker(self):
        marker = utils.generate_uuid()
        path = '/images?marker=%s' % marker
        request = unit_test_utils.get_fake_request(path)
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)

    def test_index_marker_not_valid(self):
        request = unit_test_utils.get_fake_request('/images?marker=foo')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)

    def test_index_marker(self):
        marker = utils.generate_uuid()
        path = '/images?marker=%s' % marker
        request = unit_test_utils.get_fake_request(path)
        output = self.deserializer.index(request)
        self.assertEqual(output.get('marker'), marker)

    def test_index_marker_not_specified(self):
        request = unit_test_utils.get_fake_request('/images')
        output = self.deserializer.index(request)
        self.assertFalse('marker' in output)

    def test_index_sort_key_id(self):
        request = unit_test_utils.get_fake_request('/images?sort_key=id')
        output = self.deserializer.index(request)
        self.assertEqual(output.get('sort_key'), 'id')

    def test_index_sort_dir_asc(self):
        request = unit_test_utils.get_fake_request('/images?sort_dir=asc')
        output = self.deserializer.index(request)
        self.assertEqual(output.get('sort_dir'), 'asc')

    def test_index_sort_dir_bad_value(self):
        request = unit_test_utils.get_fake_request('/images?sort_dir=blah')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.deserializer.index, request)


class TestImagesDeserializerWithExtendedSchema(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesDeserializerWithExtendedSchema, self).setUp()
        schema_api = glance.schema.API()
        props = {
            'pants': {
              'type': 'string',
              'required': True,
              'enum': ['on', 'off'],
            },
        }
        schema_api.set_custom_schema_properties('image', props)
        self.deserializer = glance.api.v2.images.RequestDeserializer(
            schema_api)

    def test_create(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'name': 'image-1', 'pants': 'on'})
        output = self.deserializer.create(request)
        expected = {
            'image': {
                'name': 'image-1',
                'properties': {'pants': 'on'},
            },
        }
        self.assertEqual(expected, output)

    def test_create_bad_data(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'name': 'image-1', 'pants': 'borked'})
        self.assertRaises(exception.InvalidObject,
                self.deserializer.create, request)

    def test_update(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'name': 'image-1', 'pants': 'off'})
        output = self.deserializer.update(request)
        expected = {
            'image': {
                'name': 'image-1',
                'properties': {'pants': 'off'},
            },
        }
        self.assertEqual(expected, output)

    def test_update_bad_data(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'name': 'image-1', 'pants': 'borked'})
        self.assertRaises(exception.InvalidObject,
                self.deserializer.update, request)


class TestImagesDeserializerWithAdditionalProperties(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesDeserializerWithAdditionalProperties, self).setUp()
        self.config(allow_additional_image_properties=True)
        schema_api = glance.schema.API()
        self.deserializer = glance.api.v2.images.RequestDeserializer(
            schema_api)

    def test_create(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'foo': 'bar'})
        output = self.deserializer.create(request)
        expected = {'image': {'properties': {'foo': 'bar'}}}
        self.assertEqual(expected, output)

    def test_create_with_additional_properties_disallowed(self):
        self.config(allow_additional_image_properties=False)
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'foo': 'bar'})
        self.assertRaises(exception.InvalidObject,
                          self.deserializer.create, request)

    def test_create_with_numeric_property(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'abc': 123})
        self.assertRaises(exception.InvalidObject,
                          self.deserializer.create, request)

    def test_create_with_list_property(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'foo': ['bar']})
        self.assertRaises(exception.InvalidObject,
                          self.deserializer.create, request)

    def test_update(self):
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'foo': 'bar'})
        output = self.deserializer.update(request)
        expected = {'image': {'properties': {'foo': 'bar'}}}
        self.assertEqual(expected, output)

    def test_update_with_additional_properties_disallowed(self):
        self.config(allow_additional_image_properties=False)
        request = unit_test_utils.get_fake_request()
        request.body = json.dumps({'foo': 'bar'})
        self.assertRaises(exception.InvalidObject,
                          self.deserializer.update, request)


class TestImagesSerializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesSerializer, self).setUp()
        schema_api = glance.schema.API()
        self.serializer = glance.api.v2.images.ResponseSerializer(schema_api)

    def test_index(self):
        fixtures = [
            {
                'id': unit_test_utils.UUID1,
                'name': 'image-1',
                'is_public': True,
                'properties': {},
                'created_at': DATETIME,
                'updated_at': DATETIME,
                'tags': ['one', 'two'],
            },
            {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'is_public': False,
                'properties': {},
                'created_at': DATETIME,
                'updated_at': DATETIME,
                'tags': [],
            },
        ]
        expected = {
            'images': [
                {
                    'id': unit_test_utils.UUID1,
                    'name': 'image-1',
                    'visibility': 'public',
                    'created_at': ISOTIME,
                    'updated_at': ISOTIME,
                    'tags': ['one', 'two'],
                    'links': [
                        {
                            'rel': 'self',
                            'href':
                                '/v2/images/%s' % unit_test_utils.UUID1,
                        },
                        {
                            'rel': 'file',
                            'href':
                                '/v2/images/%s/file' % unit_test_utils.UUID1,
                        },
                        {'rel': 'describedby', 'href': '/v2/schemas/image'}
                    ],
                },
                {
                    'id': unit_test_utils.UUID2,
                    'name': 'image-2',
                    'visibility': 'private',
                    'created_at': ISOTIME,
                    'updated_at': ISOTIME,
                    'tags': [],
                    'links': [
                        {
                            'rel': 'self',
                            'href':
                                '/v2/images/%s' % unit_test_utils.UUID2,
                        },
                        {
                            'rel': 'file',
                            'href':
                                '/v2/images/%s/file' % unit_test_utils.UUID2,
                        },
                        {'rel': 'describedby', 'href': '/v2/schemas/image'}
                    ],
                },
            ],
            'links': [],
        }
        response = webob.Response()
        self.serializer.index(response, fixtures)
        self.assertEqual(expected, json.loads(response.body))

    def test_show(self):
        fixture = {
            'id': unit_test_utils.UUID2,
            'name': 'image-2',
            'is_public': True,
            'properties': {},
            'created_at': DATETIME,
            'updated_at': DATETIME,
            'tags': ['three', 'four'],
        }
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'public',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'tags': ['three', 'four'],
                'links': [
                    {
                        'rel': 'self',
                        'href': '/v2/images/%s' % unit_test_utils.UUID2,
                    },
                    {
                        'rel': 'file',
                        'href': '/v2/images/%s/file' % unit_test_utils.UUID2,
                    },
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        self.serializer.show(response, fixture)
        self.assertEqual(expected, json.loads(response.body))

    def test_create(self):
        fixture = {
            'id': unit_test_utils.UUID2,
            'name': 'image-2',
            'is_public': False,
            'properties': {},
            'created_at': DATETIME,
            'updated_at': DATETIME,
            'tags': [],
        }
        self_link = '/v2/images/%s' % unit_test_utils.UUID2
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'private',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'tags': [],
                'links': [
                    {'rel': 'self', 'href': self_link},
                    {'rel': 'file', 'href': '%s/file' % self_link},
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        self.serializer.create(response, fixture)
        self.assertEqual(expected, json.loads(response.body))
        self.assertEqual(response.location, self_link)

    def test_update(self):
        fixture = {
            'id': unit_test_utils.UUID2,
            'name': 'image-2',
            'is_public': True,
            'properties': {},
            'created_at': DATETIME,
            'updated_at': DATETIME,
            'tags': ['five'],
        }
        self_link = '/v2/images/%s' % unit_test_utils.UUID2
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'public',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'tags': ['five'],
                'links': [
                    {'rel': 'self', 'href': self_link},
                    {'rel': 'file', 'href': '%s/file' % self_link},
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        self.serializer.update(response, fixture)
        self.assertEqual(expected, json.loads(response.body))


class TestImagesSerializerWithExtendedSchema(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesSerializerWithExtendedSchema, self).setUp()
        self.config(allow_additional_image_properties=False)
        self.schema_api = glance.schema.API()
        props = {
            'color': {
                'type': 'string',
                'required': True,
                'enum': ['red', 'green'],
            },
        }
        self.schema_api.set_custom_schema_properties('image', props)
        self.fixture = {
            'id': unit_test_utils.UUID2,
            'name': 'image-2',
            'is_public': False,
            'created_at': DATETIME,
            'updated_at': DATETIME,
            'tags': [],
            'properties': {'color': 'green', 'mood': 'grouchy'},
        }

    def test_show(self):
        serializer = glance.api.v2.images.ResponseSerializer(self.schema_api)
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'private',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'tags': [],
                'color': 'green',
                'links': [
                    {
                        'rel': 'self',
                        'href': '/v2/images/%s' % unit_test_utils.UUID2,
                    },
                    {
                        'rel': 'file',
                        'href': '/v2/images/%s/file' % unit_test_utils.UUID2,
                    },
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        serializer.show(response, self.fixture)
        self.assertEqual(expected, json.loads(response.body))

    def test_show_reports_invalid_data(self):
        serializer = glance.api.v2.images.ResponseSerializer(self.schema_api)
        self.fixture['properties']['color'] = 'invalid'
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'private',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'tags': [],
                'color': 'invalid',
                'links': [
                    {
                        'rel': 'self',
                        'href': '/v2/images/%s' % unit_test_utils.UUID2,
                    },
                    {
                        'rel': 'file',
                        'href': '/v2/images/%s/file' % unit_test_utils.UUID2,
                    },
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        serializer.show(response, self.fixture)
        self.assertEqual(expected, json.loads(response.body))


class TestImagesSerializerWithAdditionalProperties(test_utils.BaseTestCase):

    def setUp(self):
        super(TestImagesSerializerWithAdditionalProperties, self).setUp()
        self.config(allow_additional_image_properties=True)
        self.schema_api = glance.schema.API()
        self.fixture = {
            'id': unit_test_utils.UUID2,
            'name': 'image-2',
            'is_public': False,
            'created_at': DATETIME,
            'updated_at': DATETIME,
            'properties': {
                'marx': 'groucho',
            },
            'tags': [],
        }

    def test_show(self):
        serializer = glance.api.v2.images.ResponseSerializer(self.schema_api)
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'private',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'marx': 'groucho',
                'tags': [],
                'links': [
                    {
                        'rel': 'self',
                        'href': '/v2/images/%s' % unit_test_utils.UUID2,
                    },
                    {
                        'rel': 'file',
                        'href': '/v2/images/%s/file' % unit_test_utils.UUID2,
                    },
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        serializer.show(response, self.fixture)
        self.assertEqual(expected, json.loads(response.body))

    def test_show_invalid_additional_property(self):
        """Ensure that the serializer passes through invalid additional
        properties (i.e. non-string) without complaining.
        """
        serializer = glance.api.v2.images.ResponseSerializer(self.schema_api)
        self.fixture['properties']['marx'] = 123
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'private',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'marx': 123,
                'tags': [],
                'links': [
                    {
                        'rel': 'self',
                        'href': '/v2/images/%s' % unit_test_utils.UUID2,
                    },
                    {
                        'rel': 'file',
                        'href': '/v2/images/%s/file' % unit_test_utils.UUID2,
                    },
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        serializer.show(response, self.fixture)
        self.assertEqual(expected, json.loads(response.body))

    def test_show_with_additional_properties_disabled(self):
        self.config(allow_additional_image_properties=False)
        serializer = glance.api.v2.images.ResponseSerializer(self.schema_api)
        expected = {
            'image': {
                'id': unit_test_utils.UUID2,
                'name': 'image-2',
                'visibility': 'private',
                'created_at': ISOTIME,
                'updated_at': ISOTIME,
                'tags': [],
                'links': [
                    {
                        'rel': 'self',
                        'href': '/v2/images/%s' % unit_test_utils.UUID2,
                    },
                    {
                        'rel': 'file',
                        'href': '/v2/images/%s/file' % unit_test_utils.UUID2,
                    },
                    {'rel': 'describedby', 'href': '/v2/schemas/image'}
                ],
            },
        }
        response = webob.Response()
        serializer.show(response, self.fixture)
        self.assertEqual(expected, json.loads(response.body))
