#!/usr/bin/env python
# -*- coding: utf-8 -*-

##############################################################################
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
##############################################################################
import os
from gaia.core import config

try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser


base_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)))


def get_config(config_file=None):
    """
    Retrieve app configuration parameters
    such as database connections
    :return: configuration
    """
    if not config_file:
        config_file = os.path.join(base_dir, 'gaia.cfg')
    parser = ConfigParser()
    parser.read(config_file)
    config_dict = {}
    for section in parser.sections():
        config_dict[section] = {}
        for key, val in parser.items(section):
            config_dict[section][key] = val.strip('"').strip("'")
    return config_dict


config.update(get_config())


