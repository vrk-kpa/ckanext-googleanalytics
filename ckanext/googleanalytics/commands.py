import os
import re
import logging
import datetime
from collections import OrderedDict

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
        # Development commands
        elif cmd == 'test':
            self.test_queries()
        elif cmd == 'initloadtest':
            self.init_db()
            self.load_analytics(self.args)
            self.test_queries()
        elif cmd == 'migrate':
            self.migrate()
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
            print('Have you correctly run the init service task and '
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
            'metrics': 'ga:uniquePageviews, ga:entrances',
            'sort': 'ga:date',
            'dimensions': 'ga:pagePath, ga:date',
            'resolver': self.resolver_type_package,
            'save': self.save_type_package,
        }, {
            'type': 'resource',
            'dates': self.get_dates_between_update(given_start_date, ResourceStats.get_latest_update_date()),
            'filters': 'ga:pagePath=~%s' % self.resource_url_tag,
            'metrics': 'ga:uniquePageviews',
            'sort': 'ga:date',
            'dimensions': 'ga:pagePath, ga:date',
            'resolver': self.resolver_type_resource,
            'save': self.save_type_resource,
        }, {
            'type': 'visitorlocation',
            'dates': self.get_dates_between_update(given_start_date, AudienceLocationDate.get_latest_update_date()),
            'filters': ";".join(botFilters),
            'metrics': 'ga:sessions',
            'sort': 'ga:date',
            'dimensions': 'ga:country, ga:date',
            'resolver': self.resolver_type_visitorlocation,
            'save': self.save_type_visitorlocation,
        }, {
            'type': 'package_downloads',
            'dates': self.get_dates_between_update(given_start_date, PackageStats.get_latest_update_date()),
            'filters': "ga:eventCategory==Resource;ga:eventAction==Download",
            'metrics': "ga:uniqueEvents",
            'sort': "ga:date",
            'dimensions': "ga:pagePath, ga:date, ga:eventCategory",
            'resolver': self.resolver_type_package_downloads,
            'save': self.save_type_package_downloads,
        }]

        # loop through queries, parse and save them to db
        for query in queries:
            data = {}
            current = datetime.datetime.now()
            self.log.info('performing analytics query of type: %s' % query['type'])
            print 'Querying type: %s' % query['type']
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
                data = resolver(results, data)
                current = date

            save_function = query['save']
            print 'Saving type: %s' % query['type']
            save_function(data)
            model.Session.commit()
            print 'Saving done'
            self.log.info("Successfully saved analytics query of type: %s" % query['type'])

    def get_dates_between_update(self, start_date, latest_date=None):
        now = datetime.datetime.now()

        # If there is no last valid value found from database then we make sure to grab all values from start. i.e. 2014
        # We want to take minimum 2 days worth logs even latest_date is today
        floor_date = datetime.date(2014, 1, 1)

        if start_date is not None:
            floor_date = start_date
        elif latest_date is not None:
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

    def save_type_package(self, data):
        for package_id_or_name, date_collection in data.items():
            # this is a lot slower than by_name()
            item = model.Package.get(package_id_or_name)
            if not item:
                self.log.warning("Couldn't find package %s" % package_id_or_name)
                continue

            for date, value in date_collection.iteritems():
                PackageStats.update_visits(item.id, date, value["visits"], value["entrances"])

    def save_type_resource(self, data):
        for identifier, date_collection in data.items():
            resource_id = identifier
            resource = model.Session.query(model.Resource).autoflush(True).filter_by(id=resource_id).first()
            if not resource:
                self.log.warning("Couldn't find resource %s" % resource_id)
                continue
            for date, value in date_collection.iteritems():
                ResourceStats.update_visits(resource.id, date, value["downloads"])

    def save_type_package_downloads(self, data):
        for package_id_or_name, date_collection in data.items():
            package = model.Package.get(package_id_or_name)

            if not package:
                self.log.warning("Couldn't find package %s" % package_id_or_name)
                continue

            for date, value in date_collection.iteritems():
                PackageStats.update_downloads(package_id=package.id, visit_date=date, downloads=value["downloads"])

    def save_type_visitorlocation(self, data):
        for location, visits_collection in data.items():
            visits = visits_collection.get('visits', {})
            for visit_date, count in visits.iteritems():
                AudienceLocationDate.update_visits(location, visit_date, count)
                self.log.info("Updated %s on %s with %s visits" % (location, visit_date, count))

    def resolver_type_package(self, results, data):
        '''
        formats results and returns a dictionary like:
        {
            'package_name_or_id': { 2019-02-24: { 'visits': 500,  'entrances': 400, 'downloads': 300 }},
        }
        '''
        if 'rows' in results:
            for result in results.get('rows'):
                path = result[0]
                visit_date = datetime.datetime.strptime(result[1], "%Y%m%d").date()

                splitPath = path.split('/')
                path_with_vars = splitPath[splitPath.index('dataset') + 1]
                package_id_or_name = path_with_vars.split('?')[0].split('&')[0]

                visit_count = result[2]
                entrance_count = result[3]

                # add package_id_or_name if not already there
                if package_id_or_name not in data:
                    data.setdefault(package_id_or_name, {})

                if visit_date not in data[package_id_or_name]:
                    data[package_id_or_name].setdefault(visit_date, {"visits": 0, "entrances": 0})

                # Adds visits in different languages together
                data[package_id_or_name][visit_date]['visits'] += int(visit_count)
                data[package_id_or_name][visit_date]['entrances'] += int(entrance_count)

        return data

    def resolver_type_resource(self, results, data):
        '''
        formats results and returns a dictionary like:
        {
            'resource_id': { 2019-02-24: { 'downloads': 500 }},
        }
        '''
        if 'rows' in results:
            for result in results.get('rows'):
                path = result[0]
                visit_date = datetime.datetime.strptime(result[1], "%Y%m%d").date()

                splitPath = path.split('/')
                resource_id = splitPath[splitPath.index('resource') + 1]

                download_count = result[2]

                # add resource_id if not already there
                if resource_id not in data:
                    data.setdefault(resource_id, {})

                if visit_date not in data[resource_id]:
                    data[resource_id].setdefault(visit_date, {"downloads": 0})

                # Adds downloads in different languages together
                data[resource_id][visit_date]['downloads'] += int(download_count)

        return data

    def resolver_type_package_downloads(self, results, data):
        '''
        formats results and returns a dictionary like:
        {
            'package_name': { '2019-02-24': { 'downloads': 500 }, ...}, ...
        }
        '''
        if 'rows' in results:
            for result in results.get('rows'):
                path = result[0]
                visit_date = datetime.datetime.strptime(result[1], "%Y%m%d").date()
                downloads = result[3]

                splitPath = path.split('/')
                path_with_vars = splitPath[splitPath.index('dataset') + 1]
                package_name = path_with_vars.split('?')[0].split('&')[0]

                # add package if not already there
                if package_name not in data:
                    data.setdefault(package_name, {})

                # add visit_date if not already there
                if visit_date not in data[package_name]:
                    data[package_name].setdefault(visit_date, {"downloads": 0})

                # Set total downloads to
                data[package_name][visit_date]['downloads'] += int(downloads)

        return data

    def resolver_type_visitorlocation(self, results, data):
        '''
        formats results and returns a dictionary like:
        {
            'Finland': { 'visits': { 2019-02-24: 500, ... } }
        }
        '''
        if 'rows' in results:
            for result in results.get('rows'):
                location = result[0]
                date = result[1]
                count = result[2]

                visit_date = datetime.datetime.strptime(date, "%Y%m%d").date()
                # add location if not already in data
                if location not in data:
                    data.setdefault(location, {})["visits"] = {}
                data[location]['visits'][visit_date] = int(count)
        return data

    def test_queries(self):
        last_month_end = datetime.datetime.today().replace(day=1) - datetime.timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        stats = PackageStats.get_total_visits(last_month_start, last_month_end, limit=20)
        for stat in stats:
            print(stat['entrances'], stat['package_name'], stat['visits'])

    def migrate(self):
        '''
        Migrate database changes. Please note that this only adds the default values and not real data.
        Therefore, you need to separately execute the 'loadanalytics' task giving '2014-01-01' as the start_date
        parameter.
        '''
        MIGRATIONS_ADD = OrderedDict({
            "downloads": "ALTER TABLE package_stats ADD COLUMN downloads integer DEFAULT 0",
            "entrances": "ALTER TABLE package_stats ADD COLUMN entrances integer DEFAULT 0"
        })
        print("Running migrations")
        current_cols_query_packages = "select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = 'package_stats';"
        current_package_cols = list([m[0] for m in model.Session.execute(current_cols_query_packages)])
        for column, query in MIGRATIONS_ADD.iteritems():
            if column not in current_package_cols:
                print("Adding column '{0}'".format(column))
                print("Executing '{0}'".format(query))
                model.Session.execute(query)
                model.Session.commit()
