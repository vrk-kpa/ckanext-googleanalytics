import logging
from ckan.lib.base import BaseController, c, render, request

import urllib
import urllib2

import ckan.logic as logic
import hashlib
import plugin
from pylons import config

from webob.multidict import UnicodeMultiDict
from paste.util.multidict import MultiDict

from ckan.controllers.api import ApiController
from ckan.controllers.package import PackageController
import ckan.plugins as p


log = logging.getLogger('ckanext.googleanalytics')

def _post_analytics(
        user, request_obj_type, request_function, request_description, request_id, environ=None):
    environ = environ or c.environ
    if config.get('googleanalytics.id') or config.get('googleanalytics.test_mode'):
        data_dict = {
            "v": 1,
            "tid": config.get('googleanalytics.id'),
            "cid": hashlib.md5(user).hexdigest(),
            # customer id should be obfuscated
            "t": "event",
            "dh": environ['HTTP_HOST'],
            "dp": environ['PATH_INFO'],
            "dr": environ.get('HTTP_REFERER', ''),
            "ec": request_description,
            "ea": request_obj_type+request_function,
            "el": request_id,
        }
        plugin.GoogleAnalyticsPlugin.analytics_queue.put(data_dict)


class GAApiController(ApiController):

    # intercept API calls to record via google analytics
    def action(self, logic_function, ver=None):
        if c.environ['SERVER_NAME'] not in ('localhost', '127.0.0.1', '::1'):
            request_query = c.environ.get('paste.parsed_dict_querystring', ({},))[0]
            request_id = request_query.get('query', request_query.get('q', ''))
            request_description = "CKAN API Request"
            _post_analytics(c.user or 'anonymous', 'action', logic_function, request_description, request_id)
        return ApiController.action(self, logic_function, ver)


# Ugly hack since ckanext-cloudstorage replaces resource_download action
# and we can't inherit from the correct controller,
# googleanalytics needs to before cloudstorage in plugin list
OptionalController = PackageController
if p.plugin_loaded('cloudstorage'):
    from ckanext.cloudstorage.controller import StorageController
    OptionalController = StorageController


class GAResourceController(OptionalController):
    # intercept API calls to record via google analytics

    def resource_download(self, id, resource_id, filename=None):
        if c.environ['SERVER_NAME'] not in ('localhost', '127.0.0.1', '::1'):
            _post_analytics(c.user, "Resource", "Download", "CKAN Resource Download Request", resource_id)
        return OptionalController.resource_download(self, id, resource_id, filename)
