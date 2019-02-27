import logging

import urllib
import urllib2

import commands
import paste.deploy.converters as converters
import ckan.lib.helpers as h
import ckan.plugins as p
from ckanext.report.interfaces import IReport

from routes.mapper import SubMapper, Mapper as _Mapper

import threading
import Queue

log = logging.getLogger(__name__)

class GoogleAnalyticsException(Exception):
    pass

class AnalyticsPostThread(threading.Thread):
    """Threaded Url POST"""
    def __init__(self, queue, test_mode=False):
        threading.Thread.__init__(self)
        self.queue = queue
        self.test_mode = test_mode

    def run(self):
        while True:
            # grabs host from queue
            data_dict = self.queue.get()

            data = urllib.urlencode(data_dict)
            # send analytics
            if self.test_mode:
                log.info("Would send API event to Google Analytics: %s", data)
            else:
                log.debug("Sending API event to Google Analytics: %s", data)
                urllib2.urlopen(
                    "http://www.google-analytics.com/collect",
                    data,
                    # timeout in seconds
                    # https://docs.python.org/2/library/urllib2.html#urllib2.urlopen
                    10)

            # signals to queue job is done
            self.queue.task_done()

class GoogleAnalyticsPlugin(p.SingletonPlugin):

    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(IReport)

    analytics_queue = Queue.Queue()

    def configure(self, config):
        '''Load config settings for this extension from config file.

        See IConfigurable.

        '''
        test_mode = config.get('googleanalytics.test_mode')
        if test_mode:
            self.googleanalytics_id = 'test-id'
            self.googleanalytics_domain = 'test-domain'
        else:
            if 'googleanalytics.id' not in config:
                msg = "Missing googleanalytics.id in config"
                raise GoogleAnalyticsException(msg)
            self.googleanalytics_id = config['googleanalytics.id']
            self.googleanalytics_domain = config.get(
                    'googleanalytics.domain', 'auto')
        
        # If resource_prefix is not in config file then write the default value
        # to the config dict, otherwise templates seem to get 'true' when they
        # try to read resource_prefix from config.
        if 'googleanalytics_resource_prefix' not in config:
            config['googleanalytics_resource_prefix'] = (
                    commands.DEFAULT_RESOURCE_URL_TAG)
        self.googleanalytics_resource_prefix = config[
            'googleanalytics_resource_prefix']

        self.show_downloads = converters.asbool(
            config.get('googleanalytics.show_downloads', True))
        self.track_events = converters.asbool(
            config.get('googleanalytics.track_events', False))

        p.toolkit.add_resource('fanstatic_library', 'ckanext-googleanalytics')
        
        # spawn a pool of 5 threads, and pass them queue instance
        for i in range(5):
            t = AnalyticsPostThread(self.analytics_queue, test_mode=test_mode)
            t.setDaemon(True)
            t.start()

    # IConfigurer
    def update_config(self, config):
        p.toolkit.add_template_directory(config, 'templates')

    def before_map(self, map):
        '''Add new routes that this extension's controllers handle.
        
        See IRoutes.

        '''
        GET_POST = dict(method=['GET', 'POST'])

        with SubMapper(map, controller='ckanext.googleanalytics.controller:GAApiController', path_prefix='/api{ver:/3|}',
                    ver='/3') as m:
            m.connect('/action/{logic_function}', action='action', conditions=GET_POST)

        with SubMapper(map, controller='ckanext.googleanalytics.controller:GAResourceController') as m:
            m.connect('/dataset/{id}/resource/{resource_id}/download', action='resource_download')
            m.connect('/dataset/{id}/resource/{resource_id}/download/{filename}', action='resource_download')
            
        return map

    def after_map(self, map):
        '''Add new routes that this extension's controllers handle.

        See IRoutes.

        '''
        map.redirect("/analytics/dataset/top", "/data/report/analytics")
        map.connect(
            'analytics', '/analytics/dataset/top',
            controller='ckanext.googleanalytics.controller:GAController',
            action='view'
        )
        return map

    def get_helpers(self):
        '''Return the CKAN 2.0 template helper functions this plugin provides.
        See ITemplateHelpers.
        '''
        return {'googleanalytics_header': self.googleanalytics_header}
    
    def googleanalytics_header(self):
        '''Render the googleanalytics_header snippet for CKAN 2.0 templates.
        This is a template helper function that renders the
        googleanalytics_header jinja snippet. To be called from the jinja
        templates in this extension, see ITemplateHelpers.
        '''
        data = {'googleanalytics_id': self.googleanalytics_id,
                'googleanalytics_domain': self.googleanalytics_domain}
        return p.toolkit.render_snippet(
            'googleanalytics/snippets/googleanalytics_header.html', data)


    def register_reports(self):
        """Register details of an extension's reports"""
        from ckanext.googleanalytics import reports
        return [reports.googleanalytics_dataset_report_info, reports.googleanalytics_resource_report_info]
