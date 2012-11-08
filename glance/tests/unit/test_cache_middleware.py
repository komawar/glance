# Copyright 2012 OpenStack, LLC
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

import unittest
import webob

import glance.api.middleware.cache


class TestCacheMiddlewareURLMatching(unittest.TestCase):
    def test_v1_no_match_detail(self):
        req = webob.Request.blank('/v1/images/detail')
        out = glance.api.middleware.cache.CacheFilter._match_request(req)
        self.assertTrue(out is None)

    def test_v1_no_match_detail_with_query_params(self):
        req = webob.Request.blank('/v1/images/detail?limit=10')
        out = glance.api.middleware.cache.CacheFilter._match_request(req)
        self.assertTrue(out is None)

    def test_v1_match_id_with_query_param(self):
        req = webob.Request.blank('/v1/images/asdf?ping=pong')
        out = glance.api.middleware.cache.CacheFilter._match_request(req)
        self.assertEqual(out, ('v1', 'GET', 'asdf'))

    def test_v2_match_id(self):
        req = webob.Request.blank('/v2/images/asdf/file')
        out = glance.api.middleware.cache.CacheFilter._match_request(req)
        self.assertEqual(out, ('v2', 'GET', 'asdf'))

    def test_v2_no_match_bad_path(self):
        req = webob.Request.blank('/v2/images/asdf')
        out = glance.api.middleware.cache.CacheFilter._match_request(req)
        self.assertTrue(out is None)

    def test_no_match_unknown_version(self):
        req = webob.Request.blank('/v3/images/asdf')
        out = glance.api.middleware.cache.CacheFilter._match_request(req)
        self.assertTrue(out is None)


class TestCacheMiddlewareRequestStashCacheInfo(unittest.TestCase):
    def setUp(self):
        self.request = webob.Request.blank('')
        self.middleware = glance.api.middleware.cache.CacheFilter

    def test_stash_cache_request_info(self):
        self.middleware._stash_request_info(self.request, 'asdf', 'GET')
        self.assertEqual(self.request.environ['api.cache.image_id'], 'asdf')
        self.assertEqual(self.request.environ['api.cache.method'], 'GET')

    def test_fetch_cache_request_info(self):
        self.request.environ['api.cache.image_id'] = 'asdf'
        self.request.environ['api.cache.method'] = 'GET'
        (image_id, method) = self.middleware._fetch_request_info(self.request)
        self.assertEqual('asdf', image_id)
        self.assertEqual('GET', method)

    def test_fetch_cache_request_info_unset(self):
        out = self.middleware._fetch_request_info(self.request)
        self.assertEqual(out, None)


class ChecksumTestCacheFilter(glance.api.middleware.cache.CacheFilter):
    def __init__(self):
        class DummyCache(object):
            def get_caching_iter(self, image_id, image_checksum, app_iter):
                self.image_checksum = image_checksum

        self.cache = DummyCache()


class TestCacheMiddlewareChecksumVerification(unittest.TestCase):
    def test_checksum_v1_header(self):
        cache_filter = ChecksumTestCacheFilter()
        headers = {"x-image-meta-checksum": "1234567890"}
        resp = webob.Response(headers=headers)
        cache_filter._process_GET_response(resp, None)

        self.assertEqual("1234567890", cache_filter.cache.image_checksum)

    def test_checksum_v2_header(self):
        cache_filter = ChecksumTestCacheFilter()
        headers = {
            "x-image-meta-checksum": "1234567890",
            "Content-MD5": "abcdefghi"
        }
        resp = webob.Response(headers=headers)
        cache_filter._process_GET_response(resp, None)

        self.assertEqual("abcdefghi", cache_filter.cache.image_checksum)

    def test_checksum_missing_header(self):
        cache_filter = ChecksumTestCacheFilter()
        resp = webob.Response()
        cache_filter._process_GET_response(resp, None)

        self.assertEqual(None, cache_filter.cache.image_checksum)
