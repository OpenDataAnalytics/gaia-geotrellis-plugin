#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright Kitware Inc. and Epidemico Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################
import json
import os
import unittest
from gaia import formats
from gaia.parser import deserialize
from gaia.core import config

testfile_path = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), '../data')


class TestGeotrellisProcessesViaParser(unittest.TestCase):
    """Tests for the GeotrellisCloudMaskProcess via Parser"""

    def test_process_maskclouds(self):
        """Test GeotrellisCloudMaskProcess"""
        with open(os.path.join(testfile_path,
                               'mask_r_nir.json')) as inf:
            body_text = inf.read().replace('{basepath}', testfile_path)
        process = json.loads(body_text, object_hook=deserialize)
        try:
            process.compute()
            output = process.output.uri
            self.assertTrue(os.path.exists(output))
            self.assertGreaterEqual(os.path.getsize(output), 1220000)
        finally:
            if process:
                process.purge()

    def test_process_ndvi(self):
        """Test GeotrellisNDVIProcess"""
        with open(os.path.join(testfile_path,
                               'ndvi.json')) as inf:
            body_text = inf.read().replace('{basepath}', testfile_path)
        process = json.loads(body_text, object_hook=deserialize)
        try:
            process.compute()
            output = process.output.uri
            self.assertTrue(os.path.exists(output))
            self.assertGreaterEqual(os.path.getsize(output), 1220000)
        finally:
            if process:
                process.purge()
