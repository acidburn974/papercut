
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

def transfer_version():
  '''Returns the version from the change log and sets it in the source tree'''
  cl = open(path.join(here, 'CHANGES'))
  version = cl.readline().split(':')[0]
  cl.close()
  version_source = open(path.join(here, 'papercut', 'version.py'), 'w')
  version_source.write("__VERSION__ = '%s'\n" % version)
  version_source.close()
  return version

long_description = (
    "Papercut is a news server written in 100% pure Python. It is intended to be "
    "extensible to the point where people can develop their own storage plugins "
    "to integrate the NNTP protocol into their applications. Out of the box it "
    "comes with maildir and mbox storage, a simple forwarding NNTP proxy and "
    "gateway plugins for various web forums. This version of papercut has been "
    "forked from the original version found at <https://github.com/jpm/papercut> "
    "(no longer actively maintained.")

setup(
    name='papercut',

    version=transfer_version(),

    description='A pure python NNTP server extensible through plugins.',

    long_description=long_description,

    url='https://github.com/jgrassler/papercut',
    author='Joao Prado Maia',
    author_email='jpm@pessoal.org',
    maintainer='Johannes Grassler',
    maintainer_email='johannes@btw23.de',
    license='BSD',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Topic :: Communications :: Usenet News',
        'Topic :: Communications :: Email',
        'Topic :: Communications :: Email Clients (MUA)',
        'Topic :: Communications :: Mailing List Servers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
    ],

    keywords='nntp usenet gateway mail2news email maildir mbox',
    packages=find_packages(),

    install_requires = [
      'mariadb',
      'PyPika',
      'PyMySQL',
      'pyaml',        # for parsing config files
      'm9dicts',      # for deep merging configuration from multiple sources
      'requests',
      'beautifulsoup4',
    ],
    entry_points={
        'console_scripts': [
            'papercut=papercut.cmd.papercut_nntp:main',
            'papercut_config=papercut.cmd.config:main',
            'papercut_healthcheck=papercut.cmd.check_health:main',
        ],
    },
)
