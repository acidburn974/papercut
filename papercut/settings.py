# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.
# Copyright (c) 2016 Johannes Grassler. See the LICENSE file for more information.



import argparse
import m9dicts
import os
import sys
import time
import yaml

# set this to 0 (zero) for real world use
__DEBUG__ = 1
__CLIENTDEBUG__ = 1

# This module handles papercut's configuration files and command line options
# and is the canonical source of papercut's file basend and command line
# configuration. For configuration files it will take one of two approaches:
#
#   1) Absent a '--config' command line option it will attempt to load its
#      configuration from the following configuration files:
#
#        * /etc/papercut/papercut.yaml
#        * ~/papercut/papercut.yaml
#
#      Configuration settings from ~/papercut/papercut.yaml will override
#      settings from /etc/papercut/papercut.yaml if they differ.
#
#   2) If there is a '--config' command line option it will load its
#      its configuration from the file specified as its argument. Both
#      /etc/papercut/papercut.yaml and ~/papercut/papercut.yaml will be 
#      ignored in this case.
#
#   In both cases any missing configuration settings will be set to the
#   defaults defined in this module.
#
#   Configuration file settings are exposed through the CONF() method in global
#   name space. Command line options are exposed through the OPTS() in global
#   name space. CONF() returns the configuration file as a dictionary mirroring
#   the configuration files' YAML data structure. OPTS() returns the result of
#   ArgumentParser.parse_args().


# Default configuration to fall back on.
CONFIG_DEFAULT = {
  ## General configuration ##

  # Maximum number of concurrent connections
  'max_connections': 20,
  # Server log file (you can use shell environment variables)
  'log_file': "/var/log/papercut.log",
  # Domain name
  'domain_name': 'my.domain.com',
  # Host name to bind to (will also be used in NNTP responses and headers)
  'nntp_hostname': '0.0.0.0',
  # Port to listen on
  'nntp_port': 119,
  # Type of server ('read-only' or 'read-write')
  'server_type': 'read-write',
  ## Authentication settings ('yes' or 'no')
  'nntp_auth': 'no',
  # Authentication backend that Papercut will use to authenticate the users.
  'auth_backend': 'mysql',

  ## Cache settings ##

  # Whether to enable the cache system (may need a lot of diskspace). Valid
  # choices are 'yes' or 'no'.
  'nntp_cache': 'no',
  # Cache expiration interval (in seconds)
  'nntp_cache_expire': 60 * 60 * 3,
  # Path to the directory where the cache should be kept (you can use shell
  # environment variables)
  'nntp_cache_path': '/var/cache/papercut',

  ## Storage module configuration ##

  # Backend that Papercut will use to get (and store) the actual articles' content
  'storage_backend': "mariadb",
  'dbhost': "localhost",
  'dbname': "dbname",
  'dbuser': "dbuser",
  'dbpass': "dbuser",
}

# Keys that may contain a path to interpolate environment variables into.

PATH_KEYS = {
  'log_file': 1,
  'nntp_cache_path': 1,
  'mbox_path': 1,
  'maildir_path': 1,
}

# Will hold the sole authoritative instance of the Config class below.

CONFIG = None

def CONF():
  '''Helper function for convenient access to configuration'''
  if CONFIG is None:
    CONF = Config()
  return CONF.config

def OPTS():
  '''Helper function for convenient access to command line options'''
  if configuration is None:
    configuration = Config()
  return CONF.opts
  

class ConfigurationWrapper:
  '''Turns configuration dictionary's top level keys into object attributes for easier handling'''
  def __init__(self, config):
    self.__dict__.update(config)
    self._config_dict = config

  def logEvent(self, msg):
      f = open(CONF().log_file, "a")
      f.write("[%s] %s\n" % (time.strftime("%a %b %d %H:%M:%S %Y", time.gmtime()), msg))
      f.close()


class Config:
  def __init__(self):
    self.opts = self.parse_opts()
    config_files = [ '/etc/papercut/papercut.yaml']
    if self.opts.config:
      config_files = []
      for c in self.opts.config:
        if os.path.exists(c):
          config_files.append(c)
        else:
          print("WARNING: configuration file %s: no such file or directory" % c, file=sys.stderr)

    configs = [CONFIG_DEFAULT]

    for f in config_files:
      c = self.read_config(f)
      configs.append(m9dicts.make(c))

    cfg_merged = self.merge_configs(configs)
    cfg_merged = self.path_keys(cfg_merged)
    self.config = ConfigurationWrapper(cfg_merged)
    self.check_config()
    

  def parse_opts(self):
    '''Parses command line options and returns them as a dict'''
    opts = None

    if __DEBUG__ == 1:
      if os.path.basename(sys.argv[0]) == 'app':
        opts = argparse.ArgumentParser(description='%s - Papercut NNTP server' % sys.argv[0])
        opts.add_argument('-c', '--config', default=None, action='append', help="Load configuration from this file (may be specified multiple times)")

    # Distinguish between papercut and papercut_healthcheck
    if os.path.basename(sys.argv[0]) == 'papercut':
      opts = argparse.ArgumentParser(description='%s - Papercut NNTP server' % sys.argv[0])
      opts.add_argument('-c', '--config', default=None, action='append', help="Load configuration from this file (may be specified multiple times)")

    if os.path.basename(sys.argv[0]) == 'papercut_config':
      opts = argparse.ArgumentParser(description='%s - Dump merged papercut configuration on stdout' % sys.argv[0])
      opts.add_argument('-c', '--config', default=None, action='append', help="Load configuration from this file (may be specified multiple times)")

    if os.path.basename(sys.argv[0]) == 'papercut_healthcheck':
      opts = argparse.ArgumentParser(description='%s - Health check for Papercut NNTP server' % sys.argv[0])
      opts.add_argument('-c', '--config', default=None, action='append',help="Load configuration from this file (may be specified multiple times)")

    return opts.parse_args()


  def read_config(self, source):
    '''Reads configuration from file source'''
    try:
      f = open(source)
    except IOError as e:
      return {}
    return yaml.safe_load(f)


  def path_keys(self, conf):
    '''Interpolates environment and home directory into values that may contain paths'''
    for key in conf:
      if isinstance(conf[key], dict):
        conf[key] = self.path_keys(conf[key])
        continue
      if key in PATH_KEYS:
        conf[key] = os.path.expandvars(conf[key])
        conf[key] = os.path.expanduser(conf[key])
    return conf


  def check_config(self):
    '''Performs some sanity checks on a configuration dict and automatically fix some problems'''

    if self.config.storage_backend is None:
      backend_found = None

      # hierarchies with illegal names
      bad_hierarchies = []

      try:
        for h in self.config.hierarchies:
          if h.startswith('papercut'):
            bad_hierarchies.append(h)
          if 'backend' in self.config.hierarchies[h]:
            backend_found = True
      except TypeError:
        pass

      if len(bad_hierarchies) != 0:
        for h in bad_hierarchies:
          print('Illegal hierarchy name: %s (papercut* is reserved for global storage plugins)' % h,
                file=sys.stderr)
        sys.exit(1)
      if backend_found is None:
        sys.exit('No global or hierarchy specific storage backends found. ' +
                 'Please configure at least one storage backend.')

    # check for the appropriate options
    if self.config.nntp_auth == 'yes' and cfg.auth_backend == '':
        sys.exit("Please configure the 'nntp_auth' and 'auth_backend' options correctly")

  def merge_configs(self, configs):
    '''Merges a list of configuration dicts into one final configuration dict'''
    cfg = m9dicts.make()

    for config in configs:
      cfg.update(config, merge=m9dicts.MS_DICTS_AND_LISTS)

    return cfg


# helper function to log information
# TODO: Move this somewhere else
