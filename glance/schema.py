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

import copy
import json
import logging

import jsonschema

from glance.common import exception


logger = logging.getLogger(__name__)


_BASE_SCHEMA_PROPERTIES = {
    'image': {
        'id': {
            'type': 'string',
            'description': 'An identifier for the image',
            'maxLength': 36,
        },
        'name': {
            'type': 'string',
            'description': 'Descriptive name for the image',
            'maxLength': 255,
        },
        'visibility': {
            'type': 'string',
            'description': 'Scope of image accessibility',
            'enum': ['public', 'private'],
        },
    },
    'access': {
        'tenant_id': {
          'type': 'string',
          'description': 'The tenant identifier',
        },
        'can_share': {
          'type': 'boolean',
          'description': 'Ability of tenant to share with others',
          'default': False,
        },
    },
}


class ImagePropertyString(object):
    def __init__(self, name, desc, max_length=None, required=False,
                 default=None):
        self.name = name
        self.description = desc
        if max_length is not None:
            max_length = int(max_length)
        self.max_length = max_length
        self.required = required
        self.default = default

    def schema(self):
        schema = {'type': 'string', 'description': self.description}
        if self.max_length is not None:
            schema['maxLength'] = self.max_length
        if not self.required:
            schema['optional'] = True
        if self.default is not None:
            schema['default'] = self.default
        return schema
            

class ImagePropertyEnum(object):
    def __init__(self, name, desc, options, required=False, default=None):
        self.name = name
        self.description = desc
        self.options = options
        self.required = required
        self.default = default

    def schema(self):
        schema = {
            'type': 'enum',
            'description': self.description,
            'enum': self.options,
            }
        if not self.required:
            schema['optional'] = True
        if self.default is not None:
            schema['default'] = self.default
        return schema


class ImagePropertyBool(object):
    def __init__(self, name, desc, required=False, default=None):
        self.name = name
        self.description = desc
        self.required = required
        self.default = default

    def schema(self):
        schema = {
            'type': 'boolean',
            'description': self.description,
            }
        if not self.required:
            schema['optional'] = True
        if self.default is not None:
            schema['default'] = self.default
        return schema


class API(object):
    def __init__(self, conf, base_properties=_BASE_SCHEMA_PROPERTIES):
        self.conf = conf
        self.base_properties = base_properties
        self.schema_properties = copy.deepcopy(self.base_properties)

    def get_schema(self, name):
        if name == 'image' and self.conf.allow_additional_image_properties:
            additional = {'type': 'string'}
        else:
            additional = False
        return {
            'name': name,
            'properties': self.schema_properties[name],
            'additionalProperties': additional
        }

    def set_custom_schema_properties(self, schema_name, custom_properties):
        """Update the custom properties of a schema with those provided."""
        schema_properties = copy.deepcopy(self.base_properties[schema_name])

        # Ensure custom props aren't attempting to override base props
        base_keys = set(schema_properties.keys())
        custom_keys = set(custom_properties.keys())
        intersecting_keys = base_keys.intersection(custom_keys)
        conflicting_keys = [k for k in intersecting_keys
                            if schema_properties[k] != custom_properties[k]]
        if len(conflicting_keys) > 0:
            props = ', '.join(conflicting_keys)
            reason = _("custom properties (%(props)s) conflict "
                       "with base properties")
            raise exception.SchemaLoadError(reason=reason % {'props': props})

        schema_properties.update(copy.deepcopy(custom_properties))
        self.schema_properties[schema_name] = schema_properties

    def validate(self, schema_name, obj):
        schema = self.get_schema(schema_name)
        try:
            jsonschema.validate(obj, schema)
        except jsonschema.ValidationError as e:
            raise exception.InvalidObject(schema=schema_name, reason=str(e))


def read_schema_properties_file(conf, schema_name):
    """Find the schema properties files and load them into a dict."""
    schema_filename = 'schema-%s.json' % schema_name
    match = conf.find_file(schema_filename)
    if match:
        schema_file = open(match)
        schema_data = schema_file.read()
        return json.loads(schema_data)
    else:
        msg = _('Could not find schema properties file %s. Continuing '
                'without custom properties')
        logger.warn(msg % schema_filename)
        return {}


def load_custom_schema_properties(conf, api):
    """Extend base image and access schemas with custom properties."""
    for schema_name in ('image', 'access'):
        image_properties = read_schema_properties_file(conf, schema_name)
        api.set_custom_schema_properties(schema_name, image_properties)
