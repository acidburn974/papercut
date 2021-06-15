# Papercut Installation

## Requirements

* Python 2.7 (note: originally this was 2.2 but as of 0.11.0 there have been a
  bunch of changes that were only tested on 2.7.10 and may thus rely on things
  not available in the Python 2.2 standard library.)
* pip (for installation)
* Whichever package contains `mysql_config` for your system (libmysqlclient-devel
  on OpenSUSE).
* The following python modules (setuptools will take care of installing them):
  * mysql-python
  * pyaml
  * m9dicts
* Optional:
  * pyPgSQL: you will need this module for some forum storage backends if the
             forum in question is using PostgreSQL. Unfortunately this module
             is not available from pypi at the time of this writing
  * Database server (only for Phorum/phpBB/PHPNuke and/or MySQL based
    authentication)
  * Permission to add a new column to one of the Phorum tables (Phorum backend
    only)


## Installation

1) Clone the git repository (there will be a pypi package once things are a
   little more stable) from https://github.com/jgrassler/papercut.git

   git clone https://github.com/jgrassler/papercut.git /tmp/papercut

2) Create and activate a virtualenv to it (optional but recommended)

   virtualenv /tmp/pcut; . /tmp/pcut/bin/activate

3) Install papercut:

   pip install

## Configuration

Before you can run papercut you will need to create a configuration file. You
can use one of the sample configuration files from the repository's
etc/papercut directory. You can put your papercut configuration into
/etc/papercut/papercut.yaml, ~/.papercut/papercut.yaml or specify your own
location using the --config option. At a minimum you need to configure the following

* Make sure the directory the `log_file` setting points to exists and is
  writable by the user running papercut.

* Modify the `nntp_hostname` and `nntp_port` variables to your appropriate
  server name and port number. (Note: if you want to run Papercut on port 119,
  you may need to be root depending on your system)

* Choose your storage backend and change the `backend_type` setting
  accordingly. Depending on backend you will also need to modify some backend
  specific settings (see below)

### Backend specific settings

## mbox

You will need to point the `mbox_path` setting to a directory containing one or
more mbox files. Papercut will expose these files' newsgroups, one per file.

Note: This directory must only contain mbox files. No other file types or
subdirectories.

## `mysql_phorum`

* You will need to add a new column under the main forum listing table to
  associate the name of a newsgroup to a table name. Since Phorum is totally
  dynamic on the number of forums it can create, we need an extra column to
  prevent problems.

  ```
  $ cd /tmp/papercut/papercut/storage
  $ cd storage
  $ less phorum_mysql_fix.sql
  [read the information contained in the file]
  $ mysql -u username_here -p database_here < phorum_mysql_fix.sql
  [password will be requested now]
  ```

* Now that the new column was created on the main forum listing table, you 
  will need to edit it and enter the name of the newsgroup that you want for
  each forum.

* After you finish editing the main forum table, you will need to go back to
  the papercut configuration file and configure the full path for the Phorum
  settings folder. That is, the folder where you keep the `forums.php`
  configuration file and all other files that setup the options for each
  forum.

  It will usually have `forums.php`, `1.php`, `2.php` and so on. The numbers
  on the filenames are actually the forum IDs on the main forum table. In any
  case, you will need to change the `phorum_settings_path` setting in to the
  full path to this folder.

* You will also need to configure the version of the installed copy of Phorum so
  Papercut can send the correct headers when sending out copies of the posted
  articles (also called PhorumMail for the Phorum lovers out there). Set the
  `phorum_version` accordingly (i.e. `3.3.2a`).

If you find any problems with these instructions, or if the instructions didn't
work out for you, please let me know.
