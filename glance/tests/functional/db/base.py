# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010-2012 OpenStack, LLC
# Copyright 2012 Justin Santa Barbara
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

import copy
import datetime
import uuid

from glance.common import exception
from glance import context
from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils
import glance.tests.functional.db as db_tests
from glance.tests import utils as test_utils


# The default sort order of results is whatever sort key is specified,
# plus created_at and id for ties.  When we're not specifying a sort_key,
# we get the default (created_at). Some tests below expect the fixtures to be
# returned in array-order, so if if the created_at timestamps are the same,
# these tests rely on the UUID* values being in order
UUID1, UUID2, UUID3 = sorted([uuidutils.generate_uuid() for x in range(3)])


def build_image_fixture(**kwargs):
    default_datetime = timeutils.utcnow()
    image = {
        'id': uuidutils.generate_uuid(),
        'name': 'fake image #2',
        'status': 'active',
        'disk_format': 'vhd',
        'container_format': 'ovf',
        'is_public': True,
        'created_at': default_datetime,
        'updated_at': default_datetime,
        'deleted_at': None,
        'deleted': False,
        'checksum': None,
        'min_disk': 5,
        'min_ram': 256,
        'size': 19,
        'locations': [{'url': "file:///tmp/glance-tests/2", 'metadata': {}}],
        'properties': {},
    }
    image.update(kwargs)
    return image


def build_task_fixture(**kwargs):
    default_datetime = timeutils.utcnow()
    task = {
        'id': uuidutils.generate_uuid(),
        'type': 'import',
        'status': 'pending',
        'input': {'ping': 'pong'},
        'owner': uuidutils.generate_uuid(),
        'message': None,
        'expires_at': None,
        'created_at': default_datetime,
        'updated_at': default_datetime
    }
    task.update(kwargs)
    return task


class TestDriver(test_utils.BaseTestCase):

    def setUp(self):
        super(TestDriver, self).setUp()
        context_cls = context.RequestContext
        self.adm_context = context_cls(is_admin=True,
                                       auth_tok='user:user:admin')
        self.context = context_cls(is_admin=False,
                                   auth_tok='user:user:user')
        self.db_api = db_tests.get_db(self.config)
        db_tests.reset_db(self.db_api)
        self.fixtures = self.build_image_fixtures()
        self.create_images(self.fixtures)
        self.addCleanup(timeutils.clear_time_override)

    def build_image_fixtures(self):
        dt1 = timeutils.utcnow()
        dt2 = dt1 + datetime.timedelta(microseconds=5)
        fixtures = [
            {
                'id': UUID1,
                'created_at': dt1,
                'updated_at': dt1,
                'properties': {'foo': 'bar'},
                'size': 13,
            },
            {
                'id': UUID2,
                'created_at': dt1,
                'updated_at': dt2,
                'size': 17,
            },
            {
                'id': UUID3,
                'created_at': dt2,
                'updated_at': dt2,
            },
        ]
        return [build_image_fixture(**fixture) for fixture in fixtures]

    def create_images(self, images):
        for fixture in images:
            self.db_api.image_create(self.adm_context, fixture)


class DriverTests(object):

    def test_image_create_requires_status(self):
        fixture = {'name': 'mark', 'size': 12}
        self.assertRaises(exception.Invalid,
                          self.db_api.image_create, self.context, fixture)
        fixture = {'name': 'mark', 'size': 12, 'status': 'queued'}
        self.db_api.image_create(self.context, fixture)

    def test_image_create_defaults(self):
        timeutils.set_time_override()
        create_time = timeutils.utcnow()
        values = {'status': 'queued',
                  'created_at': create_time,
                  'updated_at': create_time}
        image = self.db_api.image_create(self.context, values)

        self.assertEqual(None, image['name'])
        self.assertEqual(None, image['container_format'])
        self.assertEqual(0, image['min_ram'])
        self.assertEqual(0, image['min_disk'])
        self.assertEqual(None, image['owner'])
        self.assertEqual(False, image['is_public'])
        self.assertEqual(None, image['size'])
        self.assertEqual(None, image['checksum'])
        self.assertEqual(None, image['disk_format'])
        self.assertEqual([], image['locations'])
        self.assertEqual(False, image['protected'])
        self.assertEqual(False, image['deleted'])
        self.assertEqual(None, image['deleted_at'])
        self.assertEqual([], image['properties'])
        self.assertEqual(image['created_at'], create_time)
        self.assertEqual(image['updated_at'], create_time)

        # Image IDs aren't predictable, but they should be populated
        self.assertTrue(uuid.UUID(image['id']))

        #NOTE(bcwaldon): the tags attribute should not be returned as a part
        # of a core image entity
        self.assertFalse('tags' in image)

    def test_image_create_duplicate_id(self):
        self.assertRaises(exception.Duplicate,
                          self.db_api.image_create,
                          self.context, {'id': UUID1, 'status': 'queued'})

    def test_image_create_with_locations(self):
        locations = [{'url': 'a', 'metadata': {}},
                     {'url': 'b', 'metadata': {}}]

        fixture = {'status': 'queued',
                   'locations': locations}
        image = self.db_api.image_create(self.context, fixture)
        actual = [{'url': l['url'], 'metadata': l['metadata']}
                  for l in image['locations']]
        self.assertEqual(locations, actual)

    def test_image_create_with_location_data(self):
        location_data = [{'url': 'a', 'metadata': {'key': 'value'}},
                         {'url': 'b', 'metadata': {}}]
        fixture = {'status': 'queued', 'locations': location_data}
        image = self.db_api.image_create(self.context, fixture)
        actual = [{'url': l['url'], 'metadata': l['metadata']}
                  for l in image['locations']]
        self.assertEqual(location_data, actual)

    def test_image_create_properties(self):
        fixture = {'status': 'queued', 'properties': {'ping': 'pong'}}
        image = self.db_api.image_create(self.context, fixture)
        expected = [{'name': 'ping', 'value': 'pong'}]
        actual = [{'name': p['name'], 'value': p['value']}
                  for p in image['properties']]
        self.assertEqual(expected, actual)

    def test_image_create_unknown_attribtues(self):
        fixture = {'ping': 'pong'}
        self.assertRaises(exception.Invalid,
                          self.db_api.image_create, self.context, fixture)

    def test_image_update_core_attribute(self):
        fixture = {'status': 'queued'}
        image = self.db_api.image_update(self.adm_context, UUID3, fixture)
        self.assertEqual('queued', image['status'])
        self.assertNotEqual(image['created_at'], image['updated_at'])

    def test_image_update_with_locations(self):
        locations = [{'url': 'a', 'metadata': {}},
                     {'url': 'b', 'metadata': {}}]
        fixture = {'locations': locations}
        image = self.db_api.image_update(self.adm_context, UUID3, fixture)
        self.assertEqual(locations, image['locations'])

    def test_image_update_with_location_data(self):
        location_data = [{'url': 'a', 'metadata': {'key': 'value'}},
                         {'url': 'b', 'metadata': {}}]
        fixture = {'locations': location_data}
        image = self.db_api.image_update(self.adm_context, UUID3, fixture)
        self.assertEqual(location_data, image['locations'])

    def test_image_update(self):
        fixture = {'status': 'queued', 'properties': {'ping': 'pong'}}
        image = self.db_api.image_update(self.adm_context, UUID3, fixture)
        expected = [{'name': 'ping', 'value': 'pong'}]
        actual = [{'name': p['name'], 'value': p['value']}
                  for p in image['properties']]
        self.assertEqual(expected, actual)
        self.assertEqual('queued', image['status'])
        self.assertNotEqual(image['created_at'], image['updated_at'])

    def test_image_update_properties(self):
        fixture = {'properties': {'ping': 'pong'}}
        image = self.db_api.image_update(self.adm_context, UUID1, fixture)
        expected = {'ping': 'pong', 'foo': 'bar'}
        actual = dict((p['name'], p['value']) for p in image['properties'])
        self.assertEqual(expected, actual)
        self.assertNotEqual(image['created_at'], image['updated_at'])

    def test_image_update_purge_properties(self):
        fixture = {'properties': {'ping': 'pong'}}
        image = self.db_api.image_update(self.adm_context, UUID1,
                                         fixture, purge_props=True)
        properties = dict((p['name'], p) for p in image['properties'])

        # New properties are set
        self.assertTrue('ping' in properties)
        self.assertEqual(properties['ping']['value'], 'pong')
        self.assertEqual(properties['ping']['deleted'], False)

        # Original properties still show up, but with deleted=True
        # TODO(markwash): db api should not return deleted properties
        self.assertTrue('foo' in properties)
        self.assertEqual(properties['foo']['value'], 'bar')
        self.assertEqual(properties['foo']['deleted'], True)

    def test_image_property_delete(self):
        fixture = {'name': 'ping', 'value': 'pong', 'image_id': UUID1}
        prop = self.db_api.image_property_create(self.context, fixture)
        prop = self.db_api.image_property_delete(self.context,
                                                 prop['name'], UUID1)
        self.assertNotEqual(prop['deleted_at'], None)
        self.assertTrue(prop['deleted'])

    def test_image_get(self):
        image = self.db_api.image_get(self.context, UUID1)
        self.assertEquals(image['id'], self.fixtures[0]['id'])

    def test_image_get_disallow_deleted(self):
        self.db_api.image_destroy(self.adm_context, UUID1)
        self.assertRaises(exception.NotFound, self.db_api.image_get,
                          self.context, UUID1)

    def test_image_get_allow_deleted(self):
        self.db_api.image_destroy(self.adm_context, UUID1)
        image = self.db_api.image_get(self.adm_context, UUID1)
        self.assertEquals(image['id'], self.fixtures[0]['id'])
        self.assertTrue(image['deleted'])

    def test_image_get_force_allow_deleted(self):
        self.db_api.image_destroy(self.adm_context, UUID1)
        image = self.db_api.image_get(self.context, UUID1,
                                      force_show_deleted=True)
        self.assertEquals(image['id'], self.fixtures[0]['id'])

    def test_image_get_not_owned(self):
        TENANT1 = uuidutils.generate_uuid()
        TENANT2 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        ctxt2 = context.RequestContext(is_admin=False, tenant=TENANT2,
                                       auth_tok='user:%s:user' % TENANT2)
        image = self.db_api.image_create(
                ctxt1, {'status': 'queued', 'owner': TENANT1})
        self.assertRaises(exception.Forbidden,
                          self.db_api.image_get, ctxt2, image['id'])

    def test_image_get_not_found(self):
        UUID = uuidutils.generate_uuid()
        self.assertRaises(exception.NotFound,
                          self.db_api.image_get, self.context, UUID)

    def test_image_get_all(self):
        images = self.db_api.image_get_all(self.context)
        self.assertEquals(3, len(images))

    def test_image_get_all_with_filter(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={
                                               'id': self.fixtures[0]['id'],
                                           })
        self.assertEquals(len(images), 1)
        self.assertEquals(images[0]['id'], self.fixtures[0]['id'])

    def test_image_get_all_with_filter_user_defined_property(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={'foo': 'bar'})
        self.assertEquals(len(images), 1)
        self.assertEquals(images[0]['id'], self.fixtures[0]['id'])

    def test_image_get_all_with_filter_user_deleted_property(self):
        fixture = {'name': 'poo', 'value': 'bear', 'image_id': UUID1}
        prop = self.db_api.image_property_create(self.context,
                                                 fixture)

        images = self.db_api.image_get_all(self.context,
                                           filters={
                                               'properties': {'poo': 'bear'},
                                           })
        self.assertEquals(len(images), 1)
        self.db_api.image_property_delete(self.context,
                                          prop['name'], images[0]['id'])
        images = self.db_api.image_get_all(self.context,
                                           filters={
                                               'properties': {'poo': 'bear'},
                                           })
        self.assertEquals(len(images), 0)

    def test_image_get_all_with_filter_undefined_property(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={'poo': 'bear'})
        self.assertEquals(len(images), 0)

    def test_image_get_all_size_min_max(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={
                                               'size_min': 10,
                                               'size_max': 15,
                                           })
        self.assertEquals(len(images), 1)
        self.assertEquals(images[0]['id'], self.fixtures[0]['id'])

    def test_image_get_all_size_min(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={'size_min': 15})
        self.assertEquals(len(images), 2)
        self.assertEquals(images[0]['id'], self.fixtures[2]['id'])
        self.assertEquals(images[1]['id'], self.fixtures[1]['id'])

    def test_image_get_all_size_range(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={'size_max': 15,
                                                    'size_min': 20})
        self.assertEquals(len(images), 0)

    def test_image_get_all_size_max(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={'size_max': 15})
        self.assertEquals(len(images), 1)
        self.assertEquals(images[0]['id'], self.fixtures[0]['id'])

    def test_image_get_all_with_filter_min_range_bad_value(self):
        self.assertRaises(exception.InvalidFilterRangeValue,
                          self.db_api.image_get_all,
                          self.context, filters={'size_min': 'blah'})

    def test_image_get_all_with_filter_max_range_bad_value(self):
        self.assertRaises(exception.InvalidFilterRangeValue,
                          self.db_api.image_get_all,
                          self.context, filters={'size_max': 'blah'})

    def test_image_get_all_marker(self):
        images = self.db_api.image_get_all(self.context, marker=UUID3)
        self.assertEquals(2, len(images))

    def test_image_get_all_marker_deleted(self):
        """Cannot specify a deleted image as a marker."""
        self.db_api.image_destroy(self.adm_context, UUID1)
        filters = {'deleted': False}
        self.assertRaises(exception.NotFound, self.db_api.image_get_all,
                          self.context, marker=UUID1, filters=filters)

    def test_image_get_all_marker_deleted_showing_deleted_as_admin(self):
        """Specify a deleted image as a marker if showing deleted images."""
        self.db_api.image_destroy(self.adm_context, UUID3)
        images = self.db_api.image_get_all(self.adm_context, marker=UUID3)
        #NOTE(bcwaldon): an admin should see all images (deleted or not)
        self.assertEquals(2, len(images))

    def test_image_get_all_marker_deleted_showing_deleted(self):
        """Specify a deleted image as a marker if showing deleted images.

        A non-admin user has to explicitly ask for deleted
        images, and should only see deleted images in the results
        """
        self.db_api.image_destroy(self.adm_context, UUID3)
        self.db_api.image_destroy(self.adm_context, UUID1)
        filters = {'deleted': True}
        images = self.db_api.image_get_all(self.context, marker=UUID3,
                                           filters=filters)
        self.assertEquals(1, len(images))

    def test_image_get_all_marker_null_name_desc(self):
        """Check an image with name null is handled

        Check an image with name null is handled
        marker is specified and order is descending
        """
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'name': None,
                                         'owner': TENANT1})

        images = self.db_api.image_get_all(ctxt1, marker=UUIDX,
                                           sort_key='name',
                                           sort_dir='desc')
        image_ids = [image['id'] for image in images]
        expected = []
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_marker_null_disk_format_desc(self):
        """Check an image with disk_format null is handled

        Check an image with disk_format null is handled when
        marker is specified and order is descending
        """
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'disk_format': None,
                                         'owner': TENANT1})

        images = self.db_api.image_get_all(ctxt1, marker=UUIDX,
                                           sort_key='disk_format',
                                           sort_dir='desc')
        image_ids = [image['id'] for image in images]
        expected = []
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_marker_null_container_format_desc(self):
        """Check an image with container_format null is handled

        Check an image with container_format null is handled when
        marker is specified and order is descending
        """
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'container_format': None,
                                         'owner': TENANT1})

        images = self.db_api.image_get_all(ctxt1, marker=UUIDX,
                                           sort_key='container_format',
                                           sort_dir='desc')
        image_ids = [image['id'] for image in images]
        expected = []
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_marker_null_name_asc(self):
        """Check an image with name null is handled

        Check an image with name null is handled when
        marker is specified and order is ascending
        """
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'name': None,
                                         'owner': TENANT1})

        images = self.db_api.image_get_all(ctxt1, marker=UUIDX,
                                           sort_key='name',
                                           sort_dir='asc')
        image_ids = [image['id'] for image in images]
        expected = [UUID3, UUID2, UUID1]
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_marker_null_disk_format_asc(self):
        """Check an image with disk_format null is handled

        Check an image with disk_format null is handled when
        marker is specified and order is ascending
        """
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'disk_format': None,
                                         'owner': TENANT1})

        images = self.db_api.image_get_all(ctxt1, marker=UUIDX,
                                           sort_key='disk_format',
                                           sort_dir='asc')
        image_ids = [image['id'] for image in images]
        expected = [UUID3, UUID2, UUID1]
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_marker_null_container_format_asc(self):
        """Check an image with container_format null is handled

        Check an image with container_format null is handled when
        marker is specified and order is ascending
        """
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'container_format': None,
                                         'owner': TENANT1})

        images = self.db_api.image_get_all(ctxt1, marker=UUIDX,
                                           sort_key='container_format',
                                           sort_dir='asc')
        image_ids = [image['id'] for image in images]
        expected = [UUID3, UUID2, UUID1]
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_limit(self):
        images = self.db_api.image_get_all(self.context, limit=2)
        self.assertEquals(2, len(images))

        # A limit of None should not equate to zero
        images = self.db_api.image_get_all(self.context, limit=None)
        self.assertEquals(3, len(images))

        # A limit of zero should actually mean zero
        images = self.db_api.image_get_all(self.context, limit=0)
        self.assertEquals(0, len(images))

    def test_image_get_all_owned(self):
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False,
                                       tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        image_meta_data = {'id': UUIDX, 'status': 'queued', 'owner': TENANT1}
        self.db_api.image_create(ctxt1, image_meta_data)

        TENANT2 = uuidutils.generate_uuid()
        ctxt2 = context.RequestContext(is_admin=False,
                                       tenant=TENANT2,
                                       auth_tok='user:%s:user' % TENANT2)
        UUIDY = uuidutils.generate_uuid()
        image_meta_data = {'id': UUIDY, 'status': 'queued', 'owner': TENANT2}
        self.db_api.image_create(ctxt2, image_meta_data)

        images = self.db_api.image_get_all(ctxt1)

        image_ids = [image['id'] for image in images]
        expected = [UUIDX, UUID3, UUID2, UUID1]
        self.assertEqual(sorted(expected), sorted(image_ids))

    def test_image_get_all_owned_checksum(self):
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False,
                                       tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)
        UUIDX = uuidutils.generate_uuid()
        CHECKSUM1 = '91264c3edf5972c9f1cb309543d38a5c'
        image_meta_data = {
            'id': UUIDX,
            'status': 'queued',
            'checksum': CHECKSUM1,
            'owner': TENANT1
        }
        self.db_api.image_create(ctxt1, image_meta_data)
        image_member_data = {
            'image_id': UUIDX,
            'member': TENANT1,
            'can_share': False,
            "status": "accepted",
        }
        self.db_api.image_member_create(ctxt1, image_member_data)

        TENANT2 = uuidutils.generate_uuid()
        ctxt2 = context.RequestContext(is_admin=False,
                                       tenant=TENANT2,
                                       auth_tok='user:%s:user' % TENANT2)
        UUIDY = uuidutils.generate_uuid()
        CHECKSUM2 = '92264c3edf5972c9f1cb309543d38a5c'
        image_meta_data = {
            'id': UUIDY,
            'status': 'queued',
            'checksum': CHECKSUM2,
            'owner': TENANT2
        }
        self.db_api.image_create(ctxt2, image_meta_data)
        image_member_data = {
            'image_id': UUIDY,
            'member': TENANT2,
            'can_share': False,
            "status": "accepted",
        }
        self.db_api.image_member_create(ctxt2, image_member_data)

        filters = {'visibility': 'shared', 'checksum': CHECKSUM2}
        images = self.db_api.image_get_all(ctxt2, filters)

        self.assertEquals(1, len(images))
        self.assertEqual(UUIDY, images[0]['id'])

    def test_image_get_all_with_filter_tags(self):
        self.db_api.image_tag_create(self.context, UUID1, 'x86')
        self.db_api.image_tag_create(self.context, UUID1, '64bit')
        self.db_api.image_tag_create(self.context, UUID2, 'power')
        self.db_api.image_tag_create(self.context, UUID2, '64bit')
        images = self.db_api.image_get_all(self.context,
                                           filters={'tags': ['64bit']})
        self.assertEquals(len(images), 2)
        image_ids = [image['id'] for image in images]
        expected = [UUID1, UUID2]
        self.assertEquals(sorted(expected), sorted(image_ids))

    def test_image_get_all_with_filter_multi_tags(self):
        self.db_api.image_tag_create(self.context, UUID1, 'x86')
        self.db_api.image_tag_create(self.context, UUID1, '64bit')
        self.db_api.image_tag_create(self.context, UUID2, 'power')
        self.db_api.image_tag_create(self.context, UUID2, '64bit')
        images = self.db_api.image_get_all(self.context,
                                           filters={'tags': ['64bit', 'power']
                                                    })
        self.assertEquals(len(images), 1)
        self.assertEquals(UUID2, images[0]['id'])

    def test_image_get_all_with_filter_tags_and_nonexistent(self):
        self.db_api.image_tag_create(self.context, UUID1, 'x86')
        images = self.db_api.image_get_all(self.context,
                                           filters={'tags': ['x86', 'fake']
                                                    })
        self.assertEquals(len(images), 0)

    def test_image_get_all_with_filter_deleted_tags(self):
        tag = self.db_api.image_tag_create(self.context, UUID1, 'AIX')
        images = self.db_api.image_get_all(self.context,
                                           filters={
                                               'tags': [tag],
                                           })
        self.assertEquals(len(images), 1)
        self.db_api.image_tag_delete(self.context, UUID1, tag)
        images = self.db_api.image_get_all(self.context,
                                           filters={
                                               'tags': [tag],
                                           })
        self.assertEquals(len(images), 0)

    def test_image_get_all_with_filter_undefined_tags(self):
        images = self.db_api.image_get_all(self.context,
                                           filters={'tags': ['fake']})
        self.assertEquals(len(images), 0)

    def test_image_paginate(self):
        """Paginate through a list of images using limit and marker"""
        extra_uuids = [uuidutils.generate_uuid() for i in range(2)]
        extra_images = [build_image_fixture(id=_id) for _id in extra_uuids]
        self.create_images(extra_images)

        # Reverse uuids to match default sort of created_at
        extra_uuids.reverse()

        page = self.db_api.image_get_all(self.context, limit=2)
        self.assertEquals(extra_uuids, [i['id'] for i in page])
        last = page[-1]['id']

        page = self.db_api.image_get_all(self.context, limit=2, marker=last)
        self.assertEquals([UUID3, UUID2], [i['id'] for i in page])

        page = self.db_api.image_get_all(self.context, limit=2, marker=UUID2)
        self.assertEquals([UUID1], [i['id'] for i in page])

    def test_image_get_all_invalid_sort_key(self):
        self.assertRaises(exception.InvalidSortKey, self.db_api.image_get_all,
                          self.context, sort_key='blah')

    def test_image_get_all_limit_marker(self):
        images = self.db_api.image_get_all(self.context, limit=2)
        self.assertEquals(2, len(images))

    def test_image_destroy(self):
        location_data = [{'url': 'a', 'metadata': {'key': 'value'}},
                         {'url': 'b', 'metadata': {}}]
        fixture = {'status': 'queued', 'locations': location_data}
        image = self.db_api.image_create(self.context, fixture)
        IMG_ID = image['id']

        fixture = {'name': 'ping', 'value': 'pong', 'image_id': IMG_ID}
        prop = self.db_api.image_property_create(self.context, fixture)
        TENANT2 = uuidutils.generate_uuid()
        fixture = {'image_id': IMG_ID, 'member': TENANT2, 'can_share': False}
        member = self.db_api.image_member_create(self.context, fixture)
        self.db_api.image_tag_create(self.context, IMG_ID, 'snarf')

        self.assertEqual(location_data, image['locations'])
        self.assertEquals(('ping', 'pong', IMG_ID, False),
                          (prop['name'], prop['value'],
                           prop['image_id'], prop['deleted']))
        self.assertEquals((TENANT2, IMG_ID, False),
                          (member['member'], member['image_id'],
                           member['can_share']))
        self.assertEqual(['snarf'],
                         self.db_api.image_tag_get_all(self.context, IMG_ID))

        image = self.db_api.image_destroy(self.adm_context, IMG_ID)
        self.assertTrue(image['deleted'])
        self.assertTrue(image['deleted_at'])
        self.assertRaises(exception.NotFound, self.db_api.image_get,
                          self.context, IMG_ID)

        self.assertEquals([], image['locations'])
        prop = image['properties'][0]
        self.assertEquals(('ping', IMG_ID, True),
                          (prop['name'], prop['image_id'], prop['deleted']))
        self.context.auth_tok = 'user:%s:user' % TENANT2
        members = self.db_api.image_member_find(self.context, IMG_ID)
        self.assertEquals([], members)
        tags = self.db_api.image_tag_get_all(self.context, IMG_ID)
        self.assertEquals([], tags)

    def test_image_destroy_with_delete_all(self):
        """ Check the image child element's _image_delete_all methods

        checks if all the image_delete_all methods deletes only the child
        elements of the image to be deleted.
        """
        TENANT2 = uuidutils.generate_uuid()
        location_data = [{'url': 'a', 'metadata': {'key': 'value'}},
                         {'url': 'b', 'metadata': {}}]

        def _create_image_with_child_entries():
            fixture = {'status': 'queued', 'locations': location_data}

            image_id = self.db_api.image_create(self.context, fixture)['id']

            fixture = {'name': 'ping', 'value': 'pong', 'image_id': image_id}
            self.db_api.image_property_create(self.context, fixture)
            fixture = {'image_id': image_id, 'member': TENANT2,
                       'can_share': False}
            self.db_api.image_member_create(self.context, fixture)
            self.db_api.image_tag_create(self.context, image_id, 'snarf')
            return image_id

        ACTIVE_IMG_ID = _create_image_with_child_entries()
        DEL_IMG_ID = _create_image_with_child_entries()

        deleted_image = self.db_api.image_destroy(self.adm_context, DEL_IMG_ID)
        self.assertTrue(deleted_image['deleted'])
        self.assertTrue(deleted_image['deleted_at'])
        self.assertRaises(exception.NotFound, self.db_api.image_get,
                          self.context, DEL_IMG_ID)

        active_image = self.db_api.image_get(self.context, ACTIVE_IMG_ID)
        self.assertFalse(active_image['deleted'])
        self.assertFalse(active_image['deleted_at'])

        self.assertEqual(location_data, active_image['locations'])
        self.assertEquals(1, len(active_image['properties']))
        prop = active_image['properties'][0]
        self.assertEquals(('ping', 'pong', ACTIVE_IMG_ID),
                          (prop['name'], prop['value'],
                           prop['image_id']))
        self.assertEquals((False, None),
                          (prop['deleted'], prop['deleted_at']))
        self.context.auth_tok = 'user:%s:user' % TENANT2
        members = self.db_api.image_member_find(self.context, ACTIVE_IMG_ID)
        self.assertEquals(1, len(members))
        member = members[0]
        self.assertEquals((TENANT2, ACTIVE_IMG_ID, False),
                          (member['member'], member['image_id'],
                           member['can_share']))
        tags = self.db_api.image_tag_get_all(self.context, ACTIVE_IMG_ID)
        self.assertEquals(['snarf'], tags)

    def test_image_get_multiple_members(self):
        TENANT1 = uuidutils.generate_uuid()
        TENANT2 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1,
                                       owner_is_tenant=True)
        ctxt2 = context.RequestContext(is_admin=False, user=TENANT2,
                                       auth_tok='user:%s:user' % TENANT2,
                                       owner_is_tenant=False)
        UUIDX = uuidutils.generate_uuid()
        #we need private image and context.owner should not match image owner
        self.db_api.image_create(ctxt1, {'id': UUIDX,
                                         'status': 'queued',
                                         'is_public': False,
                                         'owner': TENANT1})
        values = {'image_id': UUIDX, 'member': TENANT2, 'can_share': False}
        self.db_api.image_member_create(ctxt1, values)

        image = self.db_api.image_get(ctxt2, UUIDX)
        self.assertEquals(UUIDX, image['id'])

        # by default get_all displays only images with status 'accepted'
        images = self.db_api.image_get_all(ctxt2)
        self.assertEquals(3, len(images))

        # filter by rejected
        images = self.db_api.image_get_all(ctxt2, member_status='rejected')
        self.assertEquals(3, len(images))

        # filter by visibility
        images = self.db_api.image_get_all(ctxt2,
                                           filters={'visibility': 'shared'})
        self.assertEquals(0, len(images))

        # filter by visibility
        images = self.db_api.image_get_all(ctxt2, member_status='pending',
                                           filters={'visibility': 'shared'})
        self.assertEquals(1, len(images))

        # filter by visibility
        images = self.db_api.image_get_all(ctxt2, member_status='all',
                                           filters={'visibility': 'shared'})
        self.assertEquals(1, len(images))

        # filter by status pending
        images = self.db_api.image_get_all(ctxt2, member_status='pending')
        self.assertEquals(4, len(images))

        # filter by status all
        images = self.db_api.image_get_all(ctxt2, member_status='all')
        self.assertEquals(4, len(images))

    def test_is_image_visible(self):
        TENANT1 = uuidutils.generate_uuid()
        TENANT2 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False, tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1,
                                       owner_is_tenant=True)
        ctxt2 = context.RequestContext(is_admin=False, user=TENANT2,
                                       auth_tok='user:%s:user' % TENANT2,
                                       owner_is_tenant=False)
        UUIDX = uuidutils.generate_uuid()
        #we need private image and context.owner should not match image owner
        image = self.db_api.image_create(ctxt1, {'id': UUIDX,
                                                 'status': 'queued',
                                                 'is_public': False,
                                                 'owner': TENANT1})

        values = {'image_id': UUIDX, 'member': TENANT2, 'can_share': False}
        self.db_api.image_member_create(ctxt1, values)

        result = self.db_api.is_image_visible(ctxt2, image)
        self.assertTrue(result)

        # image should not be visible for a deleted memeber
        members = self.db_api.image_member_find(ctxt1, image_id=UUIDX)
        self.db_api.image_member_delete(ctxt1, members[0]['id'])

        result = self.db_api.is_image_visible(ctxt2, image)
        self.assertFalse(result)

    def test_image_tag_create(self):
        tag = self.db_api.image_tag_create(self.context, UUID1, 'snap')
        self.assertEqual('snap', tag)

    def test_image_tag_set_all(self):
        tags = self.db_api.image_tag_get_all(self.context, UUID1)
        self.assertEqual([], tags)

        self.db_api.image_tag_set_all(self.context, UUID1, ['ping', 'pong'])

        tags = self.db_api.image_tag_get_all(self.context, UUID1)
        #NOTE(bcwaldon): tag ordering should match exactly what was provided
        self.assertEqual(['ping', 'pong'], tags)

    def test_image_tag_get_all(self):
        self.db_api.image_tag_create(self.context, UUID1, 'snap')
        self.db_api.image_tag_create(self.context, UUID1, 'snarf')
        self.db_api.image_tag_create(self.context, UUID2, 'snarf')

        # Check the tags for the first image
        tags = self.db_api.image_tag_get_all(self.context, UUID1)
        expected = ['snap', 'snarf']
        self.assertEqual(expected, tags)

        # Check the tags for the second image
        tags = self.db_api.image_tag_get_all(self.context, UUID2)
        expected = ['snarf']
        self.assertEqual(expected, tags)

    def test_image_tag_get_all_no_tags(self):
        actual = self.db_api.image_tag_get_all(self.context, UUID1)
        self.assertEqual([], actual)

    def test_image_tag_get_all_non_existant_image(self):
        bad_image_id = uuidutils.generate_uuid()
        actual = self.db_api.image_tag_get_all(self.context, bad_image_id)
        self.assertEqual([], actual)

    def test_image_tag_delete(self):
        self.db_api.image_tag_create(self.context, UUID1, 'snap')
        self.db_api.image_tag_delete(self.context, UUID1, 'snap')
        self.assertRaises(exception.NotFound, self.db_api.image_tag_delete,
                          self.context, UUID1, 'snap')

    def test_image_member_create(self):
        timeutils.set_time_override()
        memberships = self.db_api.image_member_find(self.context)
        self.assertEqual([], memberships)

        TENANT1 = uuidutils.generate_uuid()
        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT1
        self.db_api.image_member_create(self.context,
                                        {'member': TENANT1, 'image_id': UUID1})

        memberships = self.db_api.image_member_find(self.context)
        self.assertEqual(1, len(memberships))
        actual = memberships[0]
        self.assertNotEqual(actual['created_at'], None)
        self.assertNotEqual(actual['updated_at'], None)
        actual.pop('id')
        actual.pop('created_at')
        actual.pop('updated_at')
        expected = {
            'member': TENANT1,
            'image_id': UUID1,
            'can_share': False,
            'status': 'pending',
        }
        self.assertEqual(expected, actual)

    def test_image_member_update(self):
        TENANT1 = uuidutils.generate_uuid()

        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT1
        member = self.db_api.image_member_create(self.context,
                                                 {'member': TENANT1,
                                                  'image_id': UUID1})
        member_id = member.pop('id')
        member.pop('created_at')
        member.pop('updated_at')

        expected = {'member': TENANT1,
                    'image_id': UUID1,
                    'status': 'pending',
                    'can_share': False}
        self.assertEqual(expected, member)

        member = self.db_api.image_member_update(self.context,
                                                 member_id,
                                                 {'can_share': True})

        self.assertNotEqual(member['created_at'], member['updated_at'])
        member.pop('id')
        member.pop('created_at')
        member.pop('updated_at')
        expected = {'member': TENANT1,
                    'image_id': UUID1,
                    'status': 'pending',
                    'can_share': True}
        self.assertEqual(expected, member)

        members = self.db_api.image_member_find(self.context,
                                                member=TENANT1,
                                                image_id=UUID1)
        member = members[0]
        member.pop('id')
        member.pop('created_at')
        member.pop('updated_at')
        self.assertEqual(expected, member)

    def test_image_member_update_status(self):
        TENANT1 = uuidutils.generate_uuid()
        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT1
        member = self.db_api.image_member_create(self.context,
                                                 {'member': TENANT1,
                                                  'image_id': UUID1})
        member_id = member.pop('id')
        member.pop('created_at')
        member.pop('updated_at')

        expected = {'member': TENANT1,
                    'image_id': UUID1,
                    'status': 'pending',
                    'can_share': False}
        self.assertEqual(expected, member)

        member = self.db_api.image_member_update(self.context,
                                                 member_id,
                                                 {'status': 'accepted'})

        self.assertNotEqual(member['created_at'], member['updated_at'])
        member.pop('id')
        member.pop('created_at')
        member.pop('updated_at')
        expected = {'member': TENANT1,
                    'image_id': UUID1,
                    'status': 'accepted',
                    'can_share': False}
        self.assertEqual(expected, member)

        members = self.db_api.image_member_find(self.context,
                                                member=TENANT1,
                                                image_id=UUID1)
        member = members[0]
        member.pop('id')
        member.pop('created_at')
        member.pop('updated_at')
        self.assertEqual(expected, member)

    def test_image_member_find(self):
        TENANT1 = uuidutils.generate_uuid()
        TENANT2 = uuidutils.generate_uuid()
        fixtures = [
            {'member': TENANT1, 'image_id': UUID1},
            {'member': TENANT1, 'image_id': UUID2, 'status': 'rejected'},
            {'member': TENANT2, 'image_id': UUID1, 'status': 'accepted'},
        ]
        for f in fixtures:
            self.db_api.image_member_create(self.context, copy.deepcopy(f))

        def _simplify(output):
            return

        def _assertMemberListMatch(list1, list2):
            _simple = lambda x: set([(o['member'], o['image_id']) for o in x])
            self.assertEqual(_simple(list1), _simple(list2))

        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT1
        output = self.db_api.image_member_find(self.context, member=TENANT1)
        _assertMemberListMatch([fixtures[0], fixtures[1]], output)

        output = self.db_api.image_member_find(self.adm_context,
                                               image_id=UUID1)
        _assertMemberListMatch([fixtures[0], fixtures[2]], output)

        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT2
        output = self.db_api.image_member_find(self.context,
                                               member=TENANT2,
                                               image_id=UUID1)
        _assertMemberListMatch([fixtures[2]], output)

        output = self.db_api.image_member_find(self.context,
                                               status='accepted')
        _assertMemberListMatch([fixtures[2]], output)

        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT1
        output = self.db_api.image_member_find(self.context,
                                               status='rejected')
        _assertMemberListMatch([fixtures[1]], output)

        output = self.db_api.image_member_find(self.context,
                                               status='pending')
        _assertMemberListMatch([fixtures[0]], output)

        output = self.db_api.image_member_find(self.context,
                                               status='pending',
                                               image_id=UUID2)
        _assertMemberListMatch([], output)

        image_id = uuidutils.generate_uuid()
        output = self.db_api.image_member_find(self.context,
                                               member=TENANT2,
                                               image_id=image_id)
        _assertMemberListMatch([], output)

    def test_image_member_delete(self):
        TENANT1 = uuidutils.generate_uuid()
        # NOTE(flaper87): Update auth token, otherwise
        # non visible members won't be returned.
        self.context.auth_tok = 'user:%s:user' % TENANT1
        fixture = {'member': TENANT1, 'image_id': UUID1, 'can_share': True}
        member = self.db_api.image_member_create(self.context, fixture)
        self.assertEqual(1, len(self.db_api.image_member_find(self.context)))
        member = self.db_api.image_member_delete(self.context, member['id'])
        self.assertEqual(0, len(self.db_api.image_member_find(self.context)))


class DriverQuotaTests(test_utils.BaseTestCase):

    def setUp(self):
        super(DriverQuotaTests, self).setUp()
        self.owner_id1 = uuidutils.generate_uuid()
        self.context1 = context.RequestContext(
            is_admin=False, auth_tok='user:user:user', user=self.owner_id1)
        self.db_api = db_tests.get_db(self.config)
        db_tests.reset_db(self.db_api)
        self.addCleanup(timeutils.clear_time_override)
        dt1 = timeutils.utcnow()
        dt2 = dt1 + datetime.timedelta(microseconds=5)
        fixtures = [
            {
                'id': UUID1,
                'created_at': dt1,
                'updated_at': dt1,
                'size': 13,
                'owner': self.owner_id1,
            },
            {
                'id': UUID2,
                'created_at': dt1,
                'updated_at': dt2,
                'size': 17,
                'owner': self.owner_id1,
            },
            {
                'id': UUID3,
                'created_at': dt2,
                'updated_at': dt2,
                'size': 7,
                'owner': self.owner_id1,
            },
        ]
        self.owner1_fixtures = [
            build_image_fixture(**fixture) for fixture in fixtures]

        for fixture in self.owner1_fixtures:
            self.db_api.image_create(self.context1, fixture)

    def test_storage_quota(self):
        total = reduce(lambda x, y: x + y,
                       [f['size'] for f in self.owner1_fixtures])
        x = self.db_api.user_get_storage_usage(self.context1, self.owner_id1)
        self.assertEqual(total, x)

    def test_storage_quota_without_image_id(self):
        total = reduce(lambda x, y: x + y,
                       [f['size'] for f in self.owner1_fixtures])
        total = total - self.owner1_fixtures[0]['size']
        x = self.db_api.user_get_storage_usage(
            self.context1, self.owner_id1,
            image_id=self.owner1_fixtures[0]['id'])
        self.assertEqual(total, x)

    def test_storage_quota_multiple_locations(self):
        dt1 = timeutils.utcnow()
        sz = 53
        new_fixture_dict = {'id': 'SOMEID', 'created_at': dt1,
                            'updated_at': dt1, 'size': sz,
                            'owner': self.owner_id1}
        new_fixture = build_image_fixture(**new_fixture_dict)
        new_fixture['locations'].append({'url': 'file:///some/path/file',
                                         'metadata': {}})
        self.db_api.image_create(self.context1, new_fixture)

        total = reduce(lambda x, y: x + y,
                       [f['size'] for f in self.owner1_fixtures]) + (sz * 2)
        x = self.db_api.user_get_storage_usage(self.context1, self.owner_id1)
        self.assertEqual(total, x)


class DriverTaskTests(test_utils.BaseTestCase):

    def setUp(self):
        super(DriverTaskTests, self).setUp()
        self.owner_id1 = uuidutils.generate_uuid()
        self.adm_context = context.RequestContext(is_admin=True,
                                                  auth_tok='user:user:admin')
        self.context = context.RequestContext(
            is_admin=False, auth_tok='user:user:user', user=self.owner_id1)
        self.db_api = db_tests.get_db(self.config)
        db_tests.reset_db(self.db_api)
        self.addCleanup(timeutils.clear_time_override)

    def test_task_get_all_with_filter(self):
        self.context.tenant = uuidutils.generate_uuid()
        fixtures = [
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'export',
                'input': '{"loc": "fake"}',
            }
        ]

        for fixture in fixtures:
            task = self.db_api.task_create(self.context,
                                           build_task_fixture(**fixture))

        import_tasks = self.db_api.task_get_all(self.context,
                                                filters={'type': 'import'})

        self.assertTrue(import_tasks)
        self.assertEquals(len(import_tasks), 2)
        for task in import_tasks:
            self.assertEquals(task['type'], 'import')
            self.assertEquals(task['owner'], self.context.owner)

    def test_task_get_all_as_admin(self):
        self.context.tenant = uuidutils.generate_uuid()
        fixtures = [
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': '6838eb7b-6ded-434a-882c-b344c77fe8df',
                'type': 'export',
                'input': '{"loc": "fake"}',
            }
        ]

        tasks = []
        for fixture in fixtures:
            task = self.db_api.task_create(self.context,
                                           build_task_fixture(**fixture))
            tasks.append(task)
        import_tasks = self.db_api.task_get_all(self.adm_context)
        self.assertTrue(import_tasks)
        self.assertEquals(len(import_tasks), 3)

    def test_task_get_all_marker(self):
        self.context.tenant = uuidutils.generate_uuid()
        fixtures = [
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'export',
                'input': '{"loc": "fake"}',
            }
        ]

        for fixture in fixtures:
            task = self.db_api.task_create(self.context,
                                           build_task_fixture(**fixture))
        tasks = self.db_api.task_get_all(self.context, sort_key='id')
        task_ids = [t['id'] for t in tasks]
        tasks = self.db_api.task_get_all(self.context, sort_key='id',
                                         marker=task_ids[0])
        self.assertEquals(2, len(tasks))

    def test_task_get_all_limit(self):
        self.context.tenant = uuidutils.generate_uuid()
        fixtures = [
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'import',
                'input': '{"loc": "fake"}',
            },
            {
                'owner': self.context.owner,
                'type': 'export',
                'input': '{"loc": "fake"}',
            }
        ]

        for fixture in fixtures:
            task = self.db_api.task_create(self.context,
                                           build_task_fixture(**fixture))

        tasks = self.db_api.task_get_all(self.context, limit=2)
        self.assertEquals(2, len(tasks))

        # A limit of None should not equate to zero
        tasks = self.db_api.task_get_all(self.context, limit=None)
        self.assertEquals(3, len(tasks))

        # A limit of zero should actually mean zero
        tasks = self.db_api.task_get_all(self.context, limit=0)
        self.assertEquals(0, len(tasks))

    def test_task_get_all_owned(self):
        TENANT1 = uuidutils.generate_uuid()
        ctxt1 = context.RequestContext(is_admin=False,
                                       tenant=TENANT1,
                                       auth_tok='user:%s:user' % TENANT1)

        task_values = {'type': 'import', 'status': 'pending',
                       'input': '{"loc": "fake"}', 'owner': TENANT1}
        self.db_api.task_create(ctxt1, task_values)

        TENANT2 = uuidutils.generate_uuid()
        ctxt2 = context.RequestContext(is_admin=False,
                                       tenant=TENANT2,
                                       auth_tok='user:%s:user' % TENANT2)

        task_values = {'type': 'export', 'status': 'pending',
                       'input': '{"loc": "fake"}', 'owner': TENANT2}
        self.db_api.task_create(ctxt2, task_values)

        tasks = self.db_api.task_get_all(ctxt1)

        task_owners = set([task['owner'] for task in tasks])
        expected = set([TENANT1])
        self.assertEqual(sorted(expected), sorted(task_owners))

    def test_task_get(self):
        expires_at = timeutils.utcnow()
        fixture = {
            'owner': self.context.owner,
            'type': 'import',
            'status': 'pending',
            'input': '{"loc": "fake"}',
            'expires_at': expires_at
        }

        task = self.db_api.task_create(self.context, fixture)

        self.assertIsNotNone(task)
        self.assertIsNotNone(task['id'])

        task_id = task['id']
        task = self.db_api.task_get(self.context, task_id)

        self.assertIsNotNone(task)
        self.assertEquals(task['id'], task_id)
        self.assertEquals(task['owner'], self.context.owner)
        self.assertEquals(task['type'], 'import')
        self.assertEquals(task['status'], 'pending')
        self.assertEquals(task['expires_at'], expires_at)

    def test_task_create(self):
        task_id = uuidutils.generate_uuid()
        self.context.tenant = uuidutils.generate_uuid()
        values = {
            'id': task_id,
            'owner': self.context.owner,
            'type': 'export',
            'status': 'pending',
        }
        task_values = build_task_fixture(**values)
        task = self.db_api.task_create(self.context, task_values)
        self.assertIsNotNone(task)
        self.assertEquals(task['id'], task_id)
        self.assertEquals(task['owner'], self.context.owner)
        self.assertEquals(task['type'], 'export')
        self.assertEquals(task['status'], 'pending')

    def test_task_update(self):
        self.context.tenant = uuidutils.generate_uuid()
        task_values = build_task_fixture(owner=self.context.owner)
        task = self.db_api.task_create(self.context, task_values)

        task_id = task['id']
        fixture = {'status': 'processing'}
        task = self.db_api.task_update(self.context, task_id, fixture)

        self.assertEquals(task['id'], task_id)
        self.assertEquals(task['owner'], self.context.owner)
        self.assertEquals(task['type'], 'import')
        self.assertEquals(task['status'], 'processing')

    def test_task_delete(self):
        task_values = build_task_fixture()
        task = self.db_api.task_create(self.context, task_values)

        self.assertIsNotNone(task)
        self.assertEquals(task['deleted'], False)
        self.assertIsNone(task['deleted_at'])

        task_id = task['id']
        self.db_api.task_delete(self.context, task_id)
        self.assertRaises(exception.NotFound, self.db_api.task_get,
                          self.context, task_id)


class TestVisibility(test_utils.BaseTestCase):
    def setUp(self):
        super(TestVisibility, self).setUp()
        self.db_api = db_tests.get_db(self.config)
        db_tests.reset_db(self.db_api)
        self.setup_tenants()
        self.setup_contexts()
        self.fixtures = self.build_image_fixtures()
        self.create_images(self.fixtures)

    def setup_tenants(self):
        self.admin_tenant = uuidutils.generate_uuid()
        self.tenant1 = uuidutils.generate_uuid()
        self.tenant2 = uuidutils.generate_uuid()

    def setup_contexts(self):
        self.admin_context = context.RequestContext(
                is_admin=True, tenant=self.admin_tenant)
        self.admin_none_context = context.RequestContext(
                is_admin=True, tenant=None)
        self.tenant1_context = context.RequestContext(tenant=self.tenant1)
        self.tenant2_context = context.RequestContext(tenant=self.tenant2)
        self.none_context = context.RequestContext(tenant=None)

    def build_image_fixtures(self):
        fixtures = []
        owners = {
            'Unowned': None,
            'Admin Tenant': self.admin_tenant,
            'Tenant 1': self.tenant1,
            'Tenant 2': self.tenant2,
        }
        visibilities = {'public': True, 'private': False}
        for owner_label, owner in owners.items():
            for visibility, is_public in visibilities.items():
                fixture = {
                    'name': '%s, %s' % (owner_label, visibility),
                    'owner': owner,
                    'is_public': is_public,
                }
                fixtures.append(fixture)
        return [build_image_fixture(**fixture) for fixture in fixtures]

    def create_images(self, images):
        for fixture in images:
            self.db_api.image_create(self.admin_context, fixture)


class VisibilityTests(object):

    def test_unknown_admin_sees_all(self):
        images = self.db_api.image_get_all(self.admin_none_context)
        self.assertEquals(len(images), 8)

    def test_unknown_admin_is_public_true(self):
        images = self.db_api.image_get_all(self.admin_none_context,
                                           is_public=True)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_unknown_admin_is_public_false(self):
        images = self.db_api.image_get_all(self.admin_none_context,
                                           is_public=False)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertFalse(i['is_public'])

    def test_unknown_admin_is_public_none(self):
        images = self.db_api.image_get_all(self.admin_none_context)
        self.assertEquals(len(images), 8)

    def test_unknown_admin_visibility_public(self):
        images = self.db_api.image_get_all(self.admin_none_context,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_unknown_admin_visibility_private(self):
        images = self.db_api.image_get_all(self.admin_none_context,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertFalse(i['is_public'])

    def test_known_admin_sees_all(self):
        images = self.db_api.image_get_all(self.admin_context)
        self.assertEquals(len(images), 8)

    def test_known_admin_is_public_true(self):
        images = self.db_api.image_get_all(self.admin_context, is_public=True)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_known_admin_is_public_false(self):
        images = self.db_api.image_get_all(self.admin_context,
                                           is_public=False)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertFalse(i['is_public'])

    def test_known_admin_is_public_none(self):
        images = self.db_api.image_get_all(self.admin_context)
        self.assertEquals(len(images), 8)

    def test_admin_as_user_true(self):
        images = self.db_api.image_get_all(self.admin_context,
                                           admin_as_user=True)
        self.assertEquals(len(images), 5)
        for i in images:
            self.assertTrue(i['is_public'] or i['owner'] == self.admin_tenant)

    def test_known_admin_visibility_public(self):
        images = self.db_api.image_get_all(self.admin_context,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_known_admin_visibility_private(self):
        images = self.db_api.image_get_all(self.admin_context,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertFalse(i['is_public'])

    def test_what_unknown_user_sees(self):
        images = self.db_api.image_get_all(self.none_context)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_unknown_user_is_public_true(self):
        images = self.db_api.image_get_all(self.none_context, is_public=True)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_unknown_user_is_public_false(self):
        images = self.db_api.image_get_all(self.none_context, is_public=False)
        self.assertEquals(len(images), 0)

    def test_unknown_user_is_public_none(self):
        images = self.db_api.image_get_all(self.none_context)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_unknown_user_visibility_public(self):
        images = self.db_api.image_get_all(self.none_context,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_unknown_user_visibility_private(self):
        images = self.db_api.image_get_all(self.none_context,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 0)

    def test_what_tenant1_sees(self):
        images = self.db_api.image_get_all(self.tenant1_context)
        self.assertEquals(len(images), 5)
        for i in images:
            if not i['is_public']:
                self.assertEquals(i['owner'], self.tenant1)

    def test_tenant1_is_public_true(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           is_public=True)
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_tenant1_is_public_false(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           is_public=False)
        self.assertEquals(len(images), 1)
        self.assertFalse(images[0]['is_public'])
        self.assertEquals(images[0]['owner'], self.tenant1)

    def test_tenant1_is_public_none(self):
        images = self.db_api.image_get_all(self.tenant1_context)
        self.assertEquals(len(images), 5)
        for i in images:
            if not i['is_public']:
                self.assertEquals(i['owner'], self.tenant1)

    def test_tenant1_visibility_public(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 4)
        for i in images:
            self.assertTrue(i['is_public'])

    def test_tenant1_visibility_private(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 1)
        self.assertFalse(images[0]['is_public'])
        self.assertEquals(images[0]['owner'], self.tenant1)

    def _setup_is_public_red_herring(self):
        values = {
            'name': 'Red Herring',
            'owner': self.tenant1,
            'is_public': False,
            'properties': {'is_public': 'silly'}
        }
        fixture = build_image_fixture(**values)
        self.db_api.image_create(self.admin_context, fixture)

    def test_is_public_is_a_normal_filter_for_admin(self):
        self._setup_is_public_red_herring()
        images = self.db_api.image_get_all(self.admin_context,
                                           filters={'is_public': 'silly'})
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0]['name'], 'Red Herring')

    def test_is_public_is_a_normal_filter_for_user(self):
        self._setup_is_public_red_herring()
        images = self.db_api.image_get_all(self.tenant1_context,
                                           filters={'is_public': 'silly'})
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0]['name'], 'Red Herring')

    # NOTE(markwash): the following tests are sanity checks to make sure
    # visibility filtering and is_public=(True|False) do not interact in
    # unexpected ways. However, using both of the filtering techniques
    # simultaneously is not an anticipated use case.

    def test_admin_is_public_true_and_visibility_public(self):
        images = self.db_api.image_get_all(self.admin_context, is_public=True,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 4)

    def test_admin_is_public_false_and_visibility_public(self):
        images = self.db_api.image_get_all(self.admin_context, is_public=False,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 0)

    def test_admin_is_public_true_and_visibility_private(self):
        images = self.db_api.image_get_all(self.admin_context, is_public=True,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 0)

    def test_admin_is_public_false_and_visibility_private(self):
        images = self.db_api.image_get_all(self.admin_context, is_public=False,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 4)

    def test_tenant1_is_public_true_and_visibility_public(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           is_public=True,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 4)

    def test_tenant1_is_public_false_and_visibility_public(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           is_public=False,
                                           filters={'visibility': 'public'})
        self.assertEquals(len(images), 0)

    def test_tenant1_is_public_true_and_visibility_private(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           is_public=True,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 0)

    def test_tenant1_is_public_false_and_visibility_private(self):
        images = self.db_api.image_get_all(self.tenant1_context,
                                           is_public=False,
                                           filters={'visibility': 'private'})
        self.assertEquals(len(images), 1)


class TestMembershipVisibility(test_utils.BaseTestCase):
    def setUp(self):
        super(TestMembershipVisibility, self).setUp()
        self.db_api = db_tests.get_db(self.config)
        db_tests.reset_db(self.db_api)
        self._create_contexts()
        self._create_images()

    def _create_contexts(self):
        self.owner1, self.owner1_ctx = self._user_fixture()
        self.owner2, self.owner2_ctx = self._user_fixture()
        self.tenant1, self.user1_ctx = self._user_fixture()
        self.tenant2, self.user2_ctx = self._user_fixture()
        self.tenant3, self.user3_ctx = self._user_fixture()
        self.admin_tenant, self.admin_ctx = self._user_fixture(admin=True)

    def _user_fixture(self, admin=False):
        tenant_id = uuidutils.generate_uuid()
        ctx = context.RequestContext(tenant=tenant_id, is_admin=admin)
        return tenant_id, ctx

    def _create_images(self):
        self.image_ids = {}
        for owner in [self.owner1, self.owner2]:
            self._create_image('not_shared', owner)
            self._create_image('shared-with-1', owner, members=[self.tenant1])
            self._create_image('shared-with-2', owner, members=[self.tenant2])
            self._create_image('shared-with-both', owner,
                               members=[self.tenant1, self.tenant2])

    def _create_image(self, name, owner, members=None):
        image = build_image_fixture(name=name, owner=owner, is_public=False)
        self.image_ids[(owner, name)] = image['id']
        self.db_api.image_create(self.admin_ctx, image)
        for member in members or []:
            member = {'image_id': image['id'], 'member': member}
            self.db_api.image_member_create(self.admin_ctx, member)


class MembershipVisibilityTests(object):
    def _check_by_member(self, ctx, member_id, expected):
        members = self.db_api.image_member_find(ctx, member=member_id)
        images = [self.db_api.image_get(self.admin_ctx, member['image_id'])
                  for member in members]
        facets = [(image['owner'], image['name']) for image in images]
        self.assertEqual(set(expected), set(facets))

    def test_owner1_finding_user1_memberships(self):
        """ Owner1 should see images it owns that are shared with User1 """
        expected = [
            (self.owner1, 'shared-with-1'),
            (self.owner1, 'shared-with-both'),
        ]
        self._check_by_member(self.owner1_ctx, self.tenant1, expected)

    def test_user1_finding_user1_memberships(self):
        """ User1 should see all images shared with User1 """
        expected = [
            (self.owner1, 'shared-with-1'),
            (self.owner1, 'shared-with-both'),
            (self.owner2, 'shared-with-1'),
            (self.owner2, 'shared-with-both'),
        ]
        self._check_by_member(self.user1_ctx, self.tenant1, expected)

    def test_user2_finding_user1_memberships(self):
        """ User2 should see no images shared with User1 """
        expected = []
        self._check_by_member(self.user2_ctx, self.tenant1, expected)

    def test_admin_finding_user1_memberships(self):
        """ Admin should see all images shared with User1 """
        expected = [
            (self.owner1, 'shared-with-1'),
            (self.owner1, 'shared-with-both'),
            (self.owner2, 'shared-with-1'),
            (self.owner2, 'shared-with-both'),
        ]
        self._check_by_member(self.admin_ctx, self.tenant1, expected)

    def _check_by_image(self, context, image_id, expected):
        members = self.db_api.image_member_find(context, image_id=image_id)
        member_ids = [member['member'] for member in members]
        self.assertEqual(set(expected), set(member_ids))

    def test_owner1_finding_owner1s_image_members(self):
        """ Owner1 should see all memberships of its image """
        expected = [self.tenant1, self.tenant2]
        image_id = self.image_ids[(self.owner1, 'shared-with-both')]
        self._check_by_image(self.owner1_ctx, image_id, expected)

    def test_admin_finding_owner1s_image_members(self):
        """ Admin should see all memberships of owner1's image """
        expected = [self.tenant1, self.tenant2]
        image_id = self.image_ids[(self.owner1, 'shared-with-both')]
        self._check_by_image(self.admin_ctx, image_id, expected)

    def test_user1_finding_owner1s_image_members(self):
        """ User1 should see its own membership of owner1's image """
        expected = [self.tenant1]
        image_id = self.image_ids[(self.owner1, 'shared-with-both')]
        self._check_by_image(self.user1_ctx, image_id, expected)

    def test_user2_finding_owner1s_image_members(self):
        """ User2 should see its own membership of owner1's image """
        expected = [self.tenant2]
        image_id = self.image_ids[(self.owner1, 'shared-with-both')]
        self._check_by_image(self.user2_ctx, image_id, expected)

    def test_user3_finding_owner1s_image_members(self):
        """ User3 should see no memberships of owner1's image """
        expected = []
        image_id = self.image_ids[(self.owner1, 'shared-with-both')]
        self._check_by_image(self.user3_ctx, image_id, expected)
