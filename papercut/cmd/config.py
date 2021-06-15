# Copyright (c) 2016 Johannes Grassler. See the LICENSE file for more information.
import m9dicts
import sys
import yaml

import papercut.settings

CONF = papercut.settings.CONF()

# Dumps merged configuration from one or more sources to standard output.

def main():
  yaml.dump(m9dicts.convert_to(CONF._config_dict, to_dict=True),
            sys.stdout, default_flow_style=False)

