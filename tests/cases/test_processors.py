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
import os
import unittest

from gaia.geo import RasterFileIO

from gaia_geotrellis.processes import GeotrellisMaskRedIRClouds

testfile_path = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), '../data')


class TestGeotrellisProcessors(unittest.TestCase):

    def test_process_maskclouds(self):
        """
        Test LeastCostProcess for raster inputs
        """

        inputs = [RasterFileIO(uri=f) for f in [os.path.join(testfile_path,
                             'LC81070352015218LGN00_B{}.TIF'.format(band))
                                                for band in ('4', '5', 'QA')]]

        process = GeotrellisMaskRedIRClouds(inputs=inputs, bands='')
        try:
            process.compute()
            self.assertTrue(os.path.exists(process.output.uri))
        finally:
            pass
            # if process:
            #     process.purge()
