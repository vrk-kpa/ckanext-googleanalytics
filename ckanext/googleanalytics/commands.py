import os
import re
import logging
import datetime

from pylons import config as pylonsconfig
import ckan.model as model

import ckan.plugins as p
from ckanext.googleanalytics.model import PackageStats, ResourceStats, AudienceLocationDate

PACKAGE_URL = '/dataset/'  # XXX get from routes...
DEFAULT_RESOURCE_URL_TAG = '/download/'

RESOURCE_URL_REGEX = re.compile('/dataset/[a-z0-9-_]+/resource/([a-z0-9-_]+)')
DATASET_EDIT_REGEX = re.compile('/dataset/edit/([a-z0-9-_]+)')


class GACommand(p.toolkit.CkanCommand):
    """"
    Google analytics command

    Usage::

       paster googleanalytics init
         - Creates the database tables that Google analytics expects for storing
         results

       paster googleanalytics initservice <credentials_file>
         - Initializes the service
           Where <credentials_file> is the file name containing the details
           for the service (obtained from https://console.developers.google.com/iam-admin/serviceaccounts).
           By default this is set to credentials.json

       paster googleanalytics loadanalytics <credentials_file> [start_date]
         - Parses data from Google Analytics API and stores it in our database
          <credentials file> specifies the service credentials file
          [date] specifies start date for retrieving analytics data YYYY-MM-DD format
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 0
    TEST_HOST = None
    CONFIG = None

    def __init__(self, name):
        super(GACommand, self).__init__(name)

    def command(self):
        """
        Parse command line arguments and call appropiate method.
        """
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print GACommand.__doc__
            return

        cmd = self.args[0]
        self._load_config()

        # Now we can import ckan and create logger, knowing that loggers
        # won't get disabled
        self.log = logging.getLogger('ckanext.googleanalytics')

        if cmd == 'init':
            self.init_db()
        elif cmd == 'initservice':
            self.init_service(self.args)
        elif cmd == 'loadanalytics':
            self.load_analytics(self.args)
        elif cmd == 'test':
            self.test_queries()
        elif cmd == 'initloadtest':
            self.init_db()
            self.load_analytics(self.args)
            self.test_queries()
        else:
            self.log.error('Command "%s" not recognized' % (cmd,))

    def init_db(self):
        from ckanext.googleanalytics.model import init_tables
        init_tables(model.meta.engine)

    def init_service(self, args):
        from ga_auth import init_service

        if len(args) == 1:
            raise Exception("Missing credentials file")
        credentialsfile = args[1]
        if not os.path.exists(credentialsfile):
            raise Exception('Cannot find the credentials file %s' % credentialsfile)

        try:
            self.service = init_service(credentialsfile)
        except TypeError:
            print ('Have you correctly run the init service task and '
                   'specified the correct file here')
            raise Exception('Unable to create a service')

        return self.service

    def load_analytics(self, args):
        """
        Parse data from Google Analytics API and store it
        in a local database
        """
        if not self.CONFIG:
            self._load_config()
            self.CONFIG = pylonsconfig

        self.resource_url_tag = self.CONFIG.get(
            'googleanalytics_resource_prefix',
            DEFAULT_RESOURCE_URL_TAG)

        self.parse_and_save(args)

    def ga_query(self, filters, metrics, sort, dimensions, start_date=None, end_date=None):
        """
        Get raw data from Google Analtyics.

        Returns a dictionary like::

           {'identifier': 3}
        """
        if not start_date:
            start_date = datetime.datetime(2010, 1, 1)
        start_date = start_date.strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.datetime.now()

        end_date = end_date.strftime("%Y-%m-%d")

        start_index = 1
        max_results = 10000

        print '%s -> %s' % (start_date, end_date)

        results = self.service.data().ga().get(ids='ga:%s' % self.profile_id,
                                               filters=filters,
                                               dimensions=dimensions,
                                               start_date=start_date,
                                               end_date=end_date,
                                               start_index=start_index,
                                               max_results=max_results,
                                               metrics=metrics,
                                               sort=sort
                                               ).execute()
        return results

    def parse_and_save(self, args):
        """Grab raw data from Google Analytics and save to the database"""
        from ga_auth import get_profile_id

        self.init_service(args)

        self.profile_id = get_profile_id(self.service)
        if len(args) > 3:
            raise Exception('Too many arguments')

        given_start_date = None
        if len(args) == 3:
            given_start_date = datetime.datetime.strptime(args[2], '%Y-%m-%d').date()

        botFilters = [
            'ga:browser!@StatusCake',
            'ga:browser!@Python',
            'ga:sessionDurationBucket!=0',
            'ga:sessionDurationBucket!=1',
            'ga:sessionDurationBucket!=2',
            'ga:sessionDurationBucket!=3',
            'ga:networkDomain!=ua.es',
            'ga:networkDomain!=amazonaws.com',
            'ga:networkDomain!=kcura.com',
            'ga:networkDomain!=relativity.com',
        ]
        # list of queries to send to analytics
        queries = [{
            'type': 'package',
            'dates': self.get_dates_between_update(given_start_date, PackageStats.get_latest_update_date()),
            'filters': 'ga:pagePath=~%s,ga:pagePath=~%s' % (PACKAGE_URL, self.resource_url_tag),
            'metrics': 'ga:uniquePageviews',
            'sort': '-ga:uniquePageviews',
            'dimensions': 'ga:pagePath, ga:date',
            'resolver': self.resolver_type_package,
            'save': self.save_type_package,
        }, {
            'type': 'visitorlocation',
            'dates': self.get_dates_between_update(given_start_date, AudienceLocationDate.get_latest_update_date()),
            'filters': ";".join(botFilters),
            'metrics': 'ga:sessions',
            'sort': '-ga:sessions',
            'dimensions': 'ga:country, ga:date',
            'resolver': self.resolver_type_visitorlocation,
            'save': self.save_type_visitorlocation,
        }]

        # loop through queries, parse and save them to db
        for query in queries:
            data = {}
            current = datetime.datetime.now()
            self.log.info('performing analytics query of type: %s' % query['type'])
            for date in query['dates']:
                # run query with current query values
                results = self.ga_query(start_date=date,
                                        end_date=current,
                                        filters=query['filters'],
                                        metrics=query['metrics'],
                                        sort=query['sort'],
                                        dimensions=query['dimensions'])
                # parse query
                resolver = query['resolver']
                data = resolver(query['type'], results, data)
                current = date

            save_function = query['save']
            save_function(query['type'], data)
            model.Session.commit()
            self.log.info("Successfully saved analytics query of type: %s" % query['type'])

    def get_dates_between_update(self, start_date, latest_date=None):
        now = datetime.datetime.now()

        # If there is no last valid value found from database then we make sure to grab all values from start. i.e. 2014
        # We want to take minimum 2 days worth logs even latest_date is today
        floor_date = datetime.date(2014, 1, 1)

        if start_date is not None:
            floor_date = start_date

        if latest_date is not None:
            floor_date = latest_date - datetime.timedelta(days=2)

        current_month = datetime.date(now.year, now.month, 1)
        dates = []

        # If floor date and current month belong to the same month no need to add backward months
        if current_month != datetime.date(floor_date.year, floor_date.month, 1):
            while current_month > datetime.date(floor_date.year, floor_date.month, floor_date.day):
                dates.append(current_month)
                current_month = current_month - datetime.timedelta(days=30)
        dates.append(floor_date)

        return dates

    def save_type_package(self, querytype, data):
        for identifier, visits_collection in data[querytype].items():
            visits = visits_collection.get('visits', {})
            matches = RESOURCE_URL_REGEX.match(identifier)
            if matches:
                resource_url = identifier[len(self.resource_url_tag):]
                resource = model.Session.query(model.Resource).autoflush(True) \
                    .filter_by(id=matches.group(1)).first()
                if not resource:
                    self.log.warning("Couldn't find resource %s" % resource_url)
                    continue
                for visit_date, count in visits.iteritems():
                    ResourceStats.update_visits(resource.id, visit_date, count)
                    self.log.info("Updated %s with %s visits" % (resource.id, count))
            else:
                package_name = identifier[len(PACKAGE_URL):]
                if "/" in package_name:
                    self.log.warning("%s not a valid package name" % package_name)
                    continue
                item = model.Package.by_name(package_name)
                if not item:
                    self.log.warning("Couldn't find package %s" % package_name)
                    continue
                for visit_date, count in visits.iteritems():
                    PackageStats.update_visits(item.id, visit_date, count)
                    self.log.info("Updated %s with %s visits" % (item.id, count))

    def save_type_visitorlocation(self, querytype, data):
        for location, visits_collection in data[querytype].items():
            visits = visits_collection.get('visits', {})
            for visit_date, count in visits.iteritems():
                AudienceLocationDate.update_visits(location, visit_date, count)
                self.log.info("Updated %s on %s with %s visits" % (location, visit_date, count))

    def resolver_type_package(self, querytype, results, data):
        '''
        formats results and returns a dictionary like:
        {
            'package': { 'cool-dataset-name': { 'visits': { 2019-02-24: 500, ... } }, ... },
        }
        '''
        if 'rows' in results:
            for result in results.get('rows'):
                # this is still specific for packages query
                package = result[0]
                # removes /data/ from the url
                if package.startswith('/data/'):
                    package = package[len('/data'):]

                # if package contains a language it is removed
                # the visit count for a dataset is all visits to different languages added together
                if package.startswith('/fi/') or package.startswith('/sv/') or package.startswith('/en/'):
                    package = package[len('/fi'):]

                visit_date = datetime.datetime.strptime(result[1], "%Y%m%d").date()
                count = result[2]
                # Make sure we add the different representations of the same
                # dataset /mysite.com & /www.mysite.com ...

                val = 0
                # add querytype if not already there
                if querytype not in data:
                    data.setdefault(querytype, {})
                # Adds visits in different languages together
                if package in data[querytype] and "visits" in data[querytype][package]:
                    if visit_date in data[querytype][package]['visits']:
                        val += data[querytype][package]["visits"][visit_date]
                else:
                    data[querytype].setdefault(package, {})["visits"] = {}
                data[querytype][package]['visits'][visit_date] = int(count) + val
        
        return data

    def resolver_type_visitorlocation(self, querytype, results, data):
        '''
        formats results and returns a dictionary like:
        {
            'visitorlocation': { 'Finland': { 'visits': { 2019-02-24: 500, ... } }, ... }
        }
        '''
        if 'rows' in results:
            for result in results.get('rows'):
                location = result[0]
                date = result[1]
                count = result[2]

                visit_date = datetime.datetime.strptime(date, "%Y%m%d").date()
                # add querytype if not already in data
                if querytype not in data:
                    data.setdefault(querytype, {})
                if location not in data[querytype]:
                    data[querytype].setdefault(location, {})["visits"] = {}
                data[querytype][location]['visits'][visit_date] = int(count)
        return data

    def test_queries(self):
        last_month_end = datetime.datetime.today().replace(day=1) - datetime.timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        stats = PackageStats.get_total_visits(start_date=last_month_start, end_date=last_month_end, limit=20)
        print 'stats: %s' % stats
