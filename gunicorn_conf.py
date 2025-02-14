import multiprocessing

## (large documentation of blocks here were documentation copied off the Gunicorn docs in Feb 2025.)


#
## Network Directives
# 


# The server and port.
bind = "127.0.0.1:8000"

# Set the SO_REUSEPORT flag on the listening socket.  Default: False
# reuse_port = 


#
## Worker Settings
#


# The number of worker PROCESSES.
# We are a synchronous django app, more workers is probably necessary.
workers = multiprocessing.cpu_count() * 8

# Worker THREAD count - only affects gthread (and we use gthread).  Default: 1
threads = 10

# The maximum number of simultaneous clients. This setting only affects the gthread, eventlet and
# gevent.  Default: 1000
# worker_connections = 

# Threading model for process workers.  sync, eventlet, gevent, tornado, gthread
worker_class = "gthread"


#
## Gunicorn Behavior
#


# Workers silent for more than this many seconds are killed and restarted. Value is a positive
# number or 0. Setting it to 0 has the effect of infinite timeouts by disabling timeouts for all
# workers entirely. Generally, the default of thirty seconds should suffice. Only set this
# noticeably higher if you’re sure of the repercussions for sync workers. For the non sync workers
# it just means that the worker process is still communicating and is not tied to the length of time
# required to handle a single request.
# THIS IS NOT A TIMEOUT FOR REQUESTS, IT IS A TIMEOUT FOR WORKERS. IF YOU SET THIS TO 1 AND USE SYNC
# WORKERS, YOU THEY CAN ONLY HANDLE ONE REQUEST AT A TIME AND WILL LOCK UP IF THERE ARE THAT MANY
# OPEN CONNECTIONS. GTHREADS APPEAR TO WORK JUST FINE.
timeout = 30

# The maximum number of requests a worker will process before restarting. Any value greater than
# zero will limit the number of requests a worker will process before automatically restarting. This
# is a simple method to help limit the damage of memory leaks. If this is set to zero (the default)
# then the automatic worker restarts are disabled.  Default: 0
# THIS FEATURE IS BROKEN IT WILL KILL PROCESSES ACTIVELY SERVING REQUESTS CAUSING DISCONNECTIONS.
max_requests = 0

# graceful_timeout - timeout receiving restart signal (HUP? TERM?).  Default: 30
graceful_timeout = 5

# keepalive default is 2 - the sync worker does not support this option
# keepalive = 

# The maximum number of pending connections. This refers to the number of clients that can be
# waiting to be served. Exceeding this number results in the client getting an error when attempting
# to connect. It should only affect servers under significant load. # Must be a positive integer.
# Generally set in the 64-2048 range.  Default: 2048
# backlog = 

# By preloading an application you can save some RAM resources as well as speed up server boot
# times. Although, if you defer application loading to each worker process, you can reload your
# application code easily by restarting workers.  Default: False
#
# HAS REAL EFFECTS:
# Deploy Time: t3.medium - Enabled takes ~2.5 min. Disabled - ~1 min (not done under high load, but
# had been up 10+ hours.)
#
# Memory Usage:
# Disabled - processes load into ~88MB, then ~100MB at ~first hit, 140MB after a bit of use, 
# Enabled - processes load into ~100MB, then 140MB after a bit, and then long-term processes can go
# up into the 300-400MB range.

# Process Usage Pattern:

# Enabled - Gunicorn picked 4-5 processes and literally never used the others. 10+ hours up and all
# other processes had the 100MB state. Loading up many long-term high-load processes added use of
# the next process only when the first process was handling 2 simultaneous requests - this caused
# throttling for those clients on a doubled-up process (observed ~halved data download rate).
# Disabled - processes are used more evenly, slowly loading up the 100MB and then larger memory
# usage states. utilization seems spread out.
preload_app = False


#
## application settings
#

# TODO: do we even need the wsgi.py file anymore? is there a way to get rid of it?

wsgi_app = "wsgi:application"


#
## Logging
#


# the logging level: debug info warning error critical. Default: info
loglevel = "debug"

# The logger you want to use to log events in Gunicorn. Default: 'gunicorn.glogging.Logger'
# The default class handles most normal usages in logging. It provides error and access logging. You
# can provide your own logger by giving Gunicorn a Python path to a class that quacks like [it].
logger_class = "gunicorn.glogging.Logger"

# The Access log file to write to. '-' means log to stdout. Default: None
# accesslog = "-"

# Disable redirect access logs to syslog. Default: False
# disable_redirect_access_to_syslog = 

# The access log format. Default: '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
# https://docs.gunicorn.org/en/latest/settings.html#access-log-format
# access_log_format = 

# The Error log file to write to. Using '-' for FILE makes gunicorn log to stderr. Default: '-'
# errorlog = 

# Redirect stdout/stderr to specified file in errorlog. Default: False
# capture_output =


# The log config file to use. Gunicorn uses the standard Python logging module’s Configuration file
# format.Default: None
# logconfig = 

# The log config dictionary to use, using the standard Python logging module’s dictionary
# configuration format. This option takes precedence over the logconfig and logconfig_json options,
# which uses the older file configuration format and JSON respectively. 
#    Format:  https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig
# For more context you can look at the default configuration dictionary for logging, which can be
# found at gunicorn.glogging.CONFIG_DEFAULTS.  Default: {}
# logconfig_dict = 

# The log config to read config from a JSON file. Default: None
# logconfig_json = 

# Address to send syslog messages. Default: 'udp://localhost:514'
# syslog_addr = 

# Send Gunicorn logs to syslog. Default: False
# syslog = 

# Makes Gunicorn use the parameter as program-name in the syslog entries.  All entries will be
# prefixed by gunicorn.<prefix>. By default the program name is the name of the process.
# Default: None
# syslog_prefix = 

# Syslog facility name. Default: 'user'
# syslog_facility = 


#
## 
#


# A base to use with setproctitle for process naming. This affects things like ps and top. If you’re
# going to be running more than one instance of Gunicorn you’ll probably want to set a name to tell
# them apart. This requires that you install the setproctitle module.  If not set, the
# default_proc_name setting will be used.   Default: None
proc_name = 'Gunicorn'


# Default: 'gunicorn'
# default_proc_name = 

#
## Debugging
#


# Install a trace function that spews every line executed by the server.
# spew = True


#
## Stats!
# 

# Enable inheritance for stdio file descriptors in daemon mode. Note: To disable the Python stdout
# buffering, you can to set the user environment variable PYTHONUNBUFFERED .  Default: False
# enable_stdio_inheritance = 

# The address of the StatsD server to log to.  Default: None
# `unix://PATH` for a unix domain socket. `HOST:PORT` for a network address.
# statsd_host = 

# A comma-delimited list of datadog statsd (dogstatsd) tags to append to statsd metrics.  Default: ''
# dogstatsd_tags = 

# Prefix to use when emitting statsd metrics (a trailing . is added, if not provided).  Default: ''
# statsd_prefix = 


#
## Security
#


# The maximum size of HTTP request line in bytes.  This parameter is used to limit the allowed size
# of a client’s HTTP request-line. Since the request-line consists of the HTTP method, URI, and
# protocol version, this directive places a restriction on the length of a request-URI allowed for a
# request on the server. A server needs this value to be large enough to hold any of its resource
# names, including any information that might be passed in the query part of a GET request. Value is
# a number from 0 (unlimited) to 8190.  This parameter can be used to prevent any DDOS attack.
#   Default: 4094
# limit_request_line = 

# Limit the number of HTTP headers fields in a request.  Default: 100
# limit_request_fields = 

# Limit the allowed size of an HTTP request header field. This parameter is used to limit the number
# of headers in a request to prevent DDOS attack. Used with the limit_request_field_size it allows
# more safety. By default this value is 100 and can’t be larger than 32768. Value is a positive
# number or 0. Setting it to 0 will allow unlimited header field sizes. Warning: Setting this
# parameter to a very high or unlimited value can open up for DDOS attacks.
# Default: 8190
# limit_request_field_size = 


# from pprint import pprint, pp

#
## Hooks
# 

# Called just before the master process is initialized. The callable needs to accept a single
# instance variable for the Arbiter.
# `def on_starting(server): pass`

# Called to recycle workers during a reload via SIGHUP. The callable needs to accept a single
# instance variable for the Arbiter.
def on_reload(server):
    print("on_reload called")
    # pp(vars(server))
    # exit()

# Called just after the server is started. The callable needs to accept a single instance variable
# for the Arbiter.
# `def when_ready(server): pass`

# Called just before a worker is forked. The callable needs to accept two instance variables for the
# Arbiter and new Worker.
# `def pre_fork(server, worker): pass`

# Called just after a worker has been forked. The callable needs to accept two instance variables
# for the Arbiter and new Worker.
# `def post_fork(server, worker): pass`

# Called just after a worker has initialized the application. The callable needs to accept one
# instance variable for the initialized Worker.
# `def post_worker_init(worker): pass`

# Called just after a worker exited on SIGINT or SIGQUIT. The callable needs to accept one instance
# variable for the initialized Worker.
def worker_int(worker):
    print("worker_int called")

# Called when a worker received the SIGABRT signal. This call generally happens on timeout. The
# callable needs to accept one instance variable for the initialized Worker.
def worker_abort(worker):
    print("worker_abort called...")

# Called just before a new master process is forked. The callable needs to accept a single instance
# variable for the Arbiter.
# `def pre_exec(server): pass`

# Called just before a worker processes the request. The callable needs to accept two instance
# variables for the Worker and the Request.
# `def pre_request(worker, req): worker.log.debug("%s %s", req.method, req.path)`

# Called after a worker processes the request. The callable needs to accept two instance variables
# for the Worker and the Request.
# `def post_request(worker, req, environ, resp): pass`

# Called just after a worker has been exited, in the master process. The callable needs to accept
# two instance variables for the Arbiter and the just-exited Worker.
# `def child_exit(server, worker): pass`

# Called just after a worker has been exited, in the worker process. The callable needs to accept
# two instance variables for the Arbiter and the just-exited Worker.
# `def worker_exit(server, worker): pass`

# Called just before exiting Gunicorn. The callable needs to accept a single instance variable for
# the Arbiter.
def on_exit(server): 
    print("Arbiter on_exit called")

# `ssl_context`   --   there are two examples here.
# `def ssl_context(config, default_ssl_context_factory): return default_ssl_context_factory()`
#
# Called when SSLContext is needed. Allows customizing SSL context. The callable needs to accept an
# instance variable for the Config and a factory function that returns default SSLContext which is
# initialized with certificates, private key, cert_reqs, and ciphers according to config and can be
# further customized by the callable. The callable needs to return SSLContext object. Following
# example shows a configuration file that sets the minimum TLS version to 1.3:
#
# def ssl_context(conf, default_ssl_context_factory):
#     import ssl
#     context = default_ssl_context_factory()
#     context.minimum_version = ssl.TLSVersion.TLSv1_3
#     return context


#
## Misc
#


# Enables/Disables the use of sendfile(). If not set, the value of the SENDFILE environment variable
# is used to enable or disable its usage.  Default: None
# THIS IS SOME KIND OF IO DIRECTIVE, "sendfile" IS A SYSTEM CALL.
# sendfile = 

# Manually set environment variables in the execution environment. Should be a list of strings in
# the key=value format.  Default: []
# raw_env = ["FOO=1"]


# Switch worker processes to run as this user. A valid user id (as an integer) or the name of a user
# that can be retrieved with a call to pwd.getpwnam(value) or None to not change the worker process
# user.  Default: os.geteuid()
# user = 


# Switch worker process to run as this group. A valid group id (as an integer) or the name of a user
# that can be retrieved with a call to pwd.getgrnam(value) or None to not change the worker
# processes group.
# group =


# Directory to store temporary request data as they are read. This path should be writable by the
# process permissions set for Gunicorn workers. If not specified, Gunicorn will choose a system
# generated temporary directory. Default: None
# WARNING: This may disappear in the near future.
# tmp_upload_dir = 


# `secure_scheme_headers``
# Default: {'X-FORWARDED-PROTOCOL': 'ssl', 'X-FORWARDED-PROTO': 'https', 'X-FORWARDED-SSL': 'on'}
#
# A dictionary containing headers and values that the front-end proxy uses to indicate HTTPS
# requests. If the source IP is permitted by forwarded_allow_ips (below), and at least one request
# header matches a key-value pair listed in this dictionary, then Gunicorn will set wsgi.url_scheme
# to https, so your application can tell that the request is secure.
#
# If the other headers listed in this dictionary are not present in the request, they will be
# ignored, but if the other headers are present and do not match the provided values, then the
# request will fail to parse. See the note below for more detailed examples of this behaviour.
#
# The dictionary should map upper-case header names to exact string values. The value comparisons
# are case-sensitive, unlike the header names, so make sure they’re exactly what your front-end
# proxy sends when handling HTTPS requests.
#
# It is important that your front-end proxy configuration ensures that the headers defined here can
# not be passed directly from the client.


# `forwarded_allow_ips`
# Default: '127.0.0.1,::1'
# Front-end’s IPs from which allowed to handle set secure headers. (comma separated).
#
# Set to * to disable checking of front-end IPs. This is useful for setups where you don’t know in
# advance the IP address of front-end, but instead have ensured via other means that only your
# authorized front-ends can access Gunicorn.
#
# By default, the value of the FORWARDED_ALLOW_IPS environment variable. If it is not defined, the
# default is "127.0.0.1,::1".


#
## Others, Abbreviated
#


# proxy_protocol - there is a potentially more efficient proxy protocol.
#  https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
#  https://docs.gunicorn.org/en/latest/settings.html#proxy-protocol
#
