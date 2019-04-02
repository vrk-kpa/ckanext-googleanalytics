from datetime import datetime, timedelta

from sqlalchemy import types, func, Column, ForeignKey, not_, desc
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

import ckan.model as model
import requests
from pylons import config

log = __import__('logging').getLogger(__name__)

Base = declarative_base()


class PackageStats(Base):
    """
    Contains stats for package (datasets)
    for GA tasks run against them
    Stores number of visits per all dates for each package.
    """
    __tablename__ = 'package_stats'

    package_id = Column(types.UnicodeText, nullable=False, index=True, primary_key=True)
    visit_date = Column(types.DateTime, default=datetime.now, primary_key=True)
    visits = Column(types.Integer)
    entrances = Column(types.Integer)
    downloads = Column(types.Integer)

    @classmethod
    def get(cls, id):
        return model.Session.query(cls).filter(cls.package_id == id).first()

    @classmethod
    def update_visits(cls, item_id, visit_date, visits=0, entrances=0, downloads=0):
        '''
        Updates the number of visits for a certain package_id
        or creates a new one if it is the first visit for a certain date

        :param item_id: package_id
        :param visit_date: visit date to be updated
        :param visits: number of visits during date
        :param entrances: number of entrances during date
        :return: True for a successful update, otherwise False
        '''
        package = model.Session.query(cls).filter(cls.package_id == item_id).filter(cls.visit_date == visit_date).first()
        if package is None:
            package = PackageStats(package_id=item_id, visit_date=visit_date,
                                   visits=visits, entrances=entrances, downloads=downloads)
            model.Session.add(package)
        else:
            if visits != 0:
                package.visits = visits
            if entrances != 0:
                package.entrances = entrances

        log.debug("Number of visits for date: %s updated for package id: %s", visit_date, item_id)
        model.Session.flush()
        return True

    @classmethod
    def update_downloads(cls, package_id, visit_date, downloads):
        '''
        Add's downloads amount to package, by adding downloads together.
        If package doesn't have any stats, adds stats object with empty visits and entrances
        '''
        package = model.Session.query(cls).filter(cls.package_id == package_id).filter(cls.visit_date == visit_date).first()
        if package is None:
            cls.update_visits(item_id=package_id, visit_date=visit_date, visits=0, entrances=0, downloads=downloads)
        else:
            package.downloads += downloads

        log.debug("Downloads updated for date: %s and packag: %s", visit_date, package_id)
        model.Session.flush()
        return True

    @classmethod
    def get_package_name_by_id(cls, package_id):
        package = model.Session.query(model.Package).filter(model.Package.id == package_id).first()
        pack_name = ""
        if package is not None:
            pack_name = package.title or package.name
        return pack_name

    @classmethod
    def get_visits(cls, start_date, end_date):
        '''
        Returns datasets and their visitors amount during time span, grouped by dates.

        :param start_date: Date
        :param end_date: Date
        :return: [{ visits, package_id, package_name, visit_date }, ...]
        '''
        package_visits = model.Session.query(cls) \
            .filter(cls.visit_date >= start_date) \
            .filter(cls.visit_date <= end_date) \
            .all()

        return cls.convert_to_dict(package_visits, None)

    @classmethod
    def get_total_visits(cls, start_date, end_date, limit=50, descending=True):
        '''
        Returns datasets and their visitors amount summed during time span, grouped by dataset.

        :param start_date: Date
        :param end_date: Date
        :return: [{ visits, entrances, package_id, package_name }, ...]
        '''
        def sorting_direction(value, descending):
            if descending:
                return desc(value)
            else:
                return value

        visits_by_dataset = model.Session.query(
            cls.package_id,
            func.sum(cls.visits).label('total_visits'),
            func.sum(cls.downloads).label('total_downloads'),
            func.sum(cls.entrances).label('total_entrances')
        ) \
            .filter(cls.visit_date >= start_date) \
            .filter(cls.visit_date <= end_date) \
            .group_by(cls.package_id) \
            .order_by(sorting_direction(func.sum(cls.visits), descending)) \
            .limit(limit) \
            .all()

        datasets = []
        for dataset in visits_by_dataset:
            datasets.append({
                "package_name": PackageStats.get_package_name_by_id(dataset.package_id),
                "package_id": dataset.package_id,
                "visits": dataset.total_visits,
                "entrances": dataset.total_entrances,
                "downloads": dataset.total_downloads,
            })

        return datasets

    @classmethod
    def get_visits_during_year(cls, resource_id, year):
        '''
        Returns number of visitors during one calendar yearself.
        For example, calling this with parameter year=2017 would returned
        the number of visitors during the year 2017.

        :param resource_id: ID of the resource
        :param year: Year as an integer
        :return: Number of visitors during the year
        '''
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        package_visits = model.Session.query(cls).filter(cls.package_id == resource_id) \
            .filter(cls.visit_date >= start_date) \
            .filter(cls.visit_date <= end_date) \
            .all()

        return package_visits

    @classmethod
    def get_last_visits_by_id(cls, resource_id, num_days=30):
        start_date = datetime.now() - timedelta(num_days)
        package_visits = model.Session.query(cls).filter(cls.package_id == resource_id).filter(
            cls.visit_date >= start_date).all()
        # Returns the total number of visits since the beggining of all times
        total_visits = model.Session.query(func.sum(cls.visits)).filter(cls.package_id == resource_id).scalar()
        visits = {}

        if total_visits is not None:
            visits = PackageStats.convert_to_dict(package_visits, total_visits)

        return visits

    @classmethod
    def get_top(cls, limit=20):
        package_stats = []
        # TODO: Reimplement in more efficient manner if needed (using RANK OVER and PARTITION in raw sql)
        unique_packages = model.Session.query(cls.package_id, func.count(cls.visits), func.count(cls.entrances), func.count(cls.downloads)).group_by(cls.package_id).order_by(
            func.count(cls.visits).desc()).limit(limit).all()
        # Adding last date associated to this package stat and filtering out private and deleted packages
        if unique_packages is not None:
            for package in unique_packages:
                package_id = package[0]
                visits = package[1]

                tot_package = model.Session.query(model.Package).filter(model.Package.id == package_id).filter_by(
                    state='active').filter_by(private=False).first()
                if tot_package is None:
                    continue

                last_date = model.Session.query(func.max(cls.visit_date)).filter(cls.package_id == package_id).first()

                ps = PackageStats(package_id=package_id,
                                  visit_date=last_date[0], visits=visits, entrances=package[2], downloads=package[3])
                package_stats.append(ps)
        dictat = PackageStats.convert_to_dict(package_stats, None)
        return dictat

    @classmethod
    def get_all_visits(cls, dataset_id):

        visits_dict = PackageStats.get_last_visits_by_id(dataset_id)
        resource_visits_dict = ResourceStats.get_last_visits_by_dataset_id(dataset_id)

        visit_list = []
        visits = visits_dict.get('packages', [])
        count = visits_dict.get('tot_visits', 0)

        resource_visits = resource_visits_dict.get('resources', 0)
        download_count = resource_visits_dict.get('tot_visits', 0)

        now = datetime.now() - timedelta(days=1)

        # Creates a date object for the last 30 days in the format (YEAR, MONTH, DAY)
        for d in range(0, 30):
            curr = now - timedelta(d)
            visit_list.append({'year': curr.year, 'month': curr.month, 'day': curr.day, 'visits': 0, "downloads": 0})

        for t in visits:
            visit_date_str = t['visit_date']
            if visit_date_str is not None:
                visit_date = datetime.strptime(visit_date_str, "%d-%m-%Y")
                # Build temporary match
                visit_item = next((x for x in visit_list if
                                   x['year'] == visit_date.year and x['month'] == visit_date.month and x[
                                       'day'] == visit_date.day), None)
                if visit_item:
                    visit_item['visits'] = t['visits']

        for r in resource_visits:
            visit_date_str = r['visit_date']
            if visit_date_str is not None:
                visit_date = datetime.strptime(visit_date_str, "%d-%m-%Y")
                # Build temporary match
                visit_item = next((x for x in visit_list if
                                   x['year'] == visit_date.year and x['month'] == visit_date.month and x[
                                       'day'] == visit_date.day), None)
                if visit_item:
                    visit_item['downloads'] += r['visits']

        results = {
            "visits": visit_list,
            "count": count,
            "download_count": download_count
        }
        return results

    @classmethod
    def as_dict(cls, res):
        result = {}
        package_name = PackageStats.get_package_name_by_id(res.package_id)
        result['package_name'] = package_name
        result['package_id'] = res.package_id
        result['visits'] = res.visits
        result['entrances'] = res.entrances
        result['downloads'] = res.downloads
        result['visit_date'] = res.visit_date.strftime("%d-%m-%Y")
        return result

    @classmethod
    def convert_to_dict(cls, resource_stats, tot_visits):
        visits = []
        for resource in resource_stats:
            visits.append(PackageStats.as_dict(resource))

        results = {
            "packages": visits,
        }
        if tot_visits is not None:
            results["tot_visits"] = tot_visits
        return results

    @classmethod
    def get_latest_update_date(cls):
        result = model.Session.query(cls).order_by(cls.visit_date.desc()).first()
        if result is None:
            return None
        else:
            return result.visit_date

    @classmethod
    def get_organization(cls, dataset_name):
        url = config.get("ckan.site_url") + "/data/api/3/action/package_show?id=" + dataset_name
        response = requests.get(url)
        if response:
            return response.json()['result']['organization']['name']
        else:
            response.raise_for_status()

    @classmethod
    def get_organizations_with_most_popular_datasets(cls, start_date, end_date, limit=20):
        all_packages_result = cls.get_total_visits(start_date, end_date, limit=None)
        organization_stats = {}
        for package in all_packages_result:
            package_id = package["package_id"]
            visits = package["visits"]
            downloads = package["downloads"]
            entrances = package["entrances"]

            organization_name = cls.get_organization(package_id)
            if(organization_name in organization_stats):
                organization_stats[organization_name]["visits"] += visits
                organization_stats[organization_name]["downloads"] += downloads
                organization_stats[organization_name]["entrances"] += entrances
            else:
                organization_stats[organization_name] = {
                    "visits": visits,
                    "downloads": downloads,
                    "entrances": entrances
                }

        organization_list = []
        for organization_name, stats in organization_stats.iteritems():
            organization_list.append(
                {"organization_name": organization_name,
                 "total_visits": stats["visits"],
                 "total_downloads": stats["downloads"],
                 "total_entrances": stats["entrances"]
                 }
            )

        return sorted(organization_list, key=lambda organization: organization["total_visits"], reverse=True)[:limit]


class ResourceStats(Base):
    """
    Contains stats for resources associated to a certain dataset/package
    for GA tasks run against them
    Stores number of visits i.e. downloads per all dates for each package.
    """
    __tablename__ = 'resource_stats'

    resource_id = Column(types.UnicodeText, nullable=False, index=True, primary_key=True)
    visit_date = Column(types.DateTime, default=datetime.now, primary_key=True)
    visits = Column(types.Integer)

    @classmethod
    def get(cls, id):
        return model.Session.query(cls).filter(cls.resource_id == id).first()

    @classmethod
    def update_visits(cls, item_id, visit_date, visits):
        '''
        Updates the number of visits for a certain resource_id

        :param item_id: resource_id
        :param visit_date: last visit date
        :param visits: number of visits until visit_date
        :return: True for a successful update, otherwise False
        '''
        resource = model.Session.query(cls).filter(cls.resource_id == item_id).filter(cls.visit_date == visit_date).first()
        if resource is None:
            resource = ResourceStats(resource_id=item_id, visit_date=visit_date, visits=visits)
            model.Session.add(resource)
        else:
            resource.visits = visits
            resource.visit_date = visit_date

        log.debug("Number of visits updated for resource id: %s", item_id)
        model.Session.flush()
        return True

    @classmethod
    def get_resource_info_by_id(cls, resource_id):
        resource = model.Session.query(model.Resource).filter(model.Resource.id == resource_id).first()
        res_name = None
        res_package_name = None
        res_package_id = None
        if resource is not None:
            res_package_name = resource.package.title or resource.package.name
            res_package_id = resource.package.name
            res_name = resource.description or resource.format
        return [res_name, res_package_name, res_package_id]

    @classmethod
    def get_last_visits_by_id(cls, resource_id, num_days=30):
        start_date = datetime.now() - timedelta(num_days)
        resource_visits = model.Session.query(cls).filter(cls.resource_id == resource_id).filter(
            cls.visit_date >= start_date).all()
        # Returns the total number of visits since the beggining of all times
        total_visits = model.Session.query(func.sum(cls.visits)).filter(cls.resource_id == resource_id).scalar()
        visits = {}
        if total_visits is not None:
            visits = ResourceStats.convert_to_dict(resource_visits, total_visits)
        return visits

    @classmethod
    def get_top(cls, limit=20):
        resource_stats = []
        # TODO: Reimplement in more efficient manner if needed (using RANK OVER and PARTITION in raw sql)
        unique_resources = model.Session.query(cls.resource_id, func.count(cls.visits)).group_by(cls.resource_id).order_by(
            func.count(cls.visits).desc()).limit(limit).all()
        # Adding last date associated to this package stat and filtering out private and deleted packages
        if unique_resources is not None:
            for resource in unique_resources:
                resource_id = resource[0]
                visits = resource[1]
                # TODO: Check if associated resource is private
                resource = model.Session.query(model.Resource).filter(model.Resource.id == resource_id).filter_by(
                    state='active').first()
                if resource is None:
                    continue

                last_date = model.Session.query(func.max(cls.visit_date)).filter(cls.resource_id == resource_id).first()

                rs = ResourceStats(resource_id=resource_id, visit_date=last_date[0], visits=visits)
                resource_stats.append(rs)
        dictat = ResourceStats.convert_to_dict(resource_stats, None)
        return dictat

    @classmethod
    def as_dict(cls, res):
        result = {}
        res_info = ResourceStats.get_resource_info_by_id(res.resource_id)
        result['resource_name'] = res_info[0]
        result['resource_id'] = res.resource_id
        result['package_name'] = res_info[1]
        result['package_id'] = res_info[2]
        result['visits'] = res.visits
        result['visit_date'] = res.visit_date.strftime("%d-%m-%Y")
        return result

    @classmethod
    def convert_to_dict(cls, resource_stats, tot_visits):
        visits = []
        for resource in resource_stats:
            visits.append(ResourceStats.as_dict(resource))

        results = {
            "resources": visits
        }
        if tot_visits is not None:
            results['tot_visits'] = tot_visits

        return results

    @classmethod
    def get_last_visits_by_url(cls, url, num_days=30):
        resource = model.Session.query(model.Resource).filter(model.Resource.url == url).first()
        start_date = datetime.now() - timedelta(num_days)
        # Returns the total number of visits since the beggining of all times for the associated resource to the given url
        total_visits = model.Session.query(func.sum(cls.visits)).filter(cls.resource_id == resource.id).first().scalar()
        resource_stats = model.Session.query(cls).filter(cls.resource_id == resource.id).filter(
            cls.visit_date >= start_date).all()
        visits = ResourceStats.convert_to_dict(resource_stats, total_visits)

        return visits

    @classmethod
    def get_last_visits_by_dataset_id(cls, package_id, num_days=30):
        # Fetch all resources associated to this package id
        subquery = model.Session.query(model.Resource.id).filter(model.Resource.package_id == package_id).subquery()

        start_date = datetime.now() - timedelta(num_days)
        resource_stats = model.Session.query(cls).filter(cls.resource_id.in_(subquery)).filter(
            cls.visit_date >= start_date).all()
        total_visits = model.Session.query(func.sum(cls.visits)).filter(cls.resource_id.in_(subquery)).scalar()
        visits = ResourceStats.convert_to_dict(resource_stats, total_visits)

        return visits

    @classmethod
    def get_visits_during_last_calendar_year_by_dataset_id(cls, package_id):
        # Returns a list of visits during the last calendar year.
        last_year = datetime.now().year - 1
        first_day = datetime(year=last_year, day=1, month=1)
        last_day = datetime(year=last_year, day=31, month=12)
        return cls.get_visits_by_dataset_id_between_two_dates(package_id, first_day, last_day)

    @classmethod
    def get_visits_by_dataset_id_between_two_dates(cls, package_id, start_date, end_date):
        # Returns a list of visits between the dates
        subquery = model.Session.query(model.Resource.id).filter(model.Resource.package_id == package_id).subquery()
        visits = model.Session.query(cls).filter(cls.resource_id.in_(subquery)).filter(cls.visit_date >= start_date).filter(
            cls.visit_date <= end_date).all()
        return visits

    @classmethod
    def get_all_visits(cls, id):
        visits_dict = ResourceStats.get_last_visits_by_id(id)
        count = visits_dict.get('tot_visits', 0)
        visits = visits_dict.get('resources', [])
        visit_list = []

        now = datetime.now() - timedelta(days=1)

        # Creates a temporary date object for the last 30 days in the format (YEAR, MONTH, DAY, #visits this day)
        # If there is no entry for a certain date should return 0 visits
        for d in range(0, 30):
            curr = now - timedelta(d)
            visit_list.append({'year': curr.year, 'month': curr.month, 'day': curr.day, 'visits': 0})

        for t in visits:
            visit_date_str = t['visit_date']
            if visit_date_str is not None:
                visit_date = datetime.strptime(visit_date_str, "%d-%m-%Y")
                # Build temporary match
                visit_item = next((x for x in visit_list if
                                   x['year'] == visit_date.year and x['month'] == visit_date.month and x[
                                       'day'] == visit_date.day), None)
                if visit_item:
                    visit_item['visits'] = t['visits']

        results = {
            "downloads": visit_list,
            "count": count
        }
        return results

    @classmethod
    def get_latest_update_date(cls):
        result = model.Session.query(cls).order_by(cls.visit_date.desc()).first()
        if result is None:
            return None
        else:
            return result.visit_date


class AudienceLocation(Base):
    """
    Contains stats for different visitors locations
    Stores all different countries.
    """
    __tablename__ = 'audience_location'

    id = Column(types.Integer, primary_key=True, autoincrement=True, unique=True)
    location_name = Column(types.UnicodeText, nullable=False, primary_key=True)

    visits_by_date = relationship("AudienceLocationDate", back_populates="location")

    @classmethod
    def get(cls, id):
        return model.Session.query(cls).filter(cls.id == id).first()

    @classmethod
    def update_location(cls, location_name):
        # Check if the location can be found
        location = model.Session.query(cls).filter(cls.location_name == location_name)
        if location is None:
            # Add location if not in db
            location = AudienceLocation(location_name=location_name)
            model.Session.add(location)
            log.debug("New location added: %s", location_name)
        else:
            location.location_name = location_name
            log.debug("Location name updated: %s", location_name)

        model.Session.flush()
        return True


class AudienceLocationDate(Base):
    """
    Contains stats for different visitors locations by date
    Maps user amounts to dates and locations
    """
    __tablename__ = 'audience_location_date'

    id = Column(types.Integer, primary_key=True, autoincrement=True, unique=True)
    date = Column(types.DateTime, default=datetime.now, primary_key=True)

    visits = Column(types.Integer)
    location_id = Column(types.Integer, ForeignKey('audience_location.id'))

    location = relationship("AudienceLocation", back_populates="visits_by_date")

    @classmethod
    def update_visits(cls, location_name, visit_date, visits):
        '''
        Updates the number of visits for a certain date and location

        :param location_name: location_name
        :param date: last visit date
        :param visits: number of visits until visit_date
        :return: True for a successful update, otherwise False
        '''
        # find location_id by name
        location = model.Session.query(AudienceLocation).filter(AudienceLocation.location_name == location_name).first()

        # if not found add location as new location
        if location is None:
            location = AudienceLocation(location_name=location_name)
            model.Session.add(location)
            model.Session.commit()

        # find if location already has views for that date
        location_by_date = model.Session.query(cls).filter(cls.location_id == location.id).filter(
            cls.date == visit_date).first()
        # if not add them as a new row
        if location_by_date is None:
            location_by_date = AudienceLocationDate(location_id=location.id, date=visit_date, visits=visits)
            model.Session.add(location_by_date)
        else:
            location_by_date.visits = visits

        log.debug("Number of visits updated for location %s" % location_name)
        model.Session.flush()
        return True

    @classmethod
    def get_visits(cls, start_date, end_date):
        '''
        Get all visits items between selected dates
        grouped by date

        returns list of dicst like:
        [
            {
                location_name
                date
                visits
            }
        ]
        '''
        data = model.Session.query(cls.visits, cls.date, cls.location_id) \
            .filter(cls.date >= start_date) \
            .filter(cls.date <= end_date) \
            .order_by(cls.date.desc()) \
            .all()

        return cls.convert_list_to_dicts(data)

    @classmethod
    def get_total_visits(cls, start_date, end_date):
        '''
        Returns the total amount of visits on the website
        from the start date to the end date

        return dict like:
        {
            total_visits
        }
        '''
        total_visits = model.Session.query(func.sum(cls.visits)) \
            .filter(cls.date >= start_date) \
            .filter(cls.date <= end_date) \
            .scalar()

        return {"total_visits": total_visits}

    @classmethod
    def get_total_visits_by_location(cls, start_date, end_date, location_name):
        '''
        Returns amount of visits in the location
        from start_date to end_date

        ! in the beginning of location_name works as NOT

        !Finland = not Finland
        returns everything that is not Finland

        returns list of dicst like:
        [
            {
                location_name
                total_visits
            }
        ]
        '''

        negate = False
        if location_name.startswith('!'):
            location_name = location_name[1:]
            negate = True

        location_id = cls.get_location_id_by_name(location_name)

        total_visits = model.Session.query(func.sum(cls.visits)).filter(maybe_negate(cls.location_id, location_id, negate)) \
            .filter(cls.date >= start_date) \
            .filter(cls.date <= end_date) \
            .scalar()

        return {"total_visits": total_visits}

    @classmethod
    def get_total_top_locations(cls, limit=20):
        '''
        Locations sorted by total visits

        returns list of dicts like:
        [
            {
                location_name
                total_visits
            }
        ]
        '''
        locations = model.Session.query(cls.location_id, func.sum(cls.visits).label('total_visits')) \
            .group_by(cls.location_id) \
            .order_by(func.sum(cls.visits).desc()) \
            .limit(limit) \
            .all()

        return cls.convert_list_to_dicts(locations)

    @classmethod
    def special_total_location_to_rest(cls, start_date, end_date, location):
        location_details = cls.get_total_visits_by_location(start_date, end_date, location)
        location_details['location_name'] = location
        rest = cls.get_total_visits_by_location(start_date, end_date, '!' + location)
        rest['location_name'] = 'Other'

        return [
            location_details,
            rest,
        ]

    @classmethod
    def special_total_by_months(cls, start_date=None, end_date=None):
        if end_date is None:
            # last day of last month
            end_date = datetime.today().replace(day=1) - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=365)  # one year

        visits = cls.get_visits(start_date=start_date, end_date=end_date)

        unique_months = []
        results = []

        for item in visits:
            combined_date = str(item['date'].month) + '-' + str(item['date'].year)
            if combined_date in unique_months:
                for x in results:
                    if x['combined_date'] == combined_date:
                        x['visits'] += item['visits']
            else:
                unique_months.append(combined_date)
                results.append({'combined_date': combined_date, 'date': item['date'].__str__(), 'visits': item['visits']})

        results.sort(key=lambda x: x['date'])

        return results

    @classmethod
    def get_location_name_by_id(cls, location_id):
        location = model.Session.query(AudienceLocation).filter(AudienceLocation.id == location_id).first()
        return location.location_name

    @classmethod
    def get_location_id_by_name(cls, location_name):
        location = model.Session.query(AudienceLocation).filter(AudienceLocation.location_name == location_name).first()
        location_id = []
        if location is not None:
            location_id = location.id
        return location_id

    @classmethod
    def get_latest_update_date(cls):
        result = model.Session.query(cls).order_by(cls.date.desc()).first()
        if result is None:
            return None
        else:
            return result.date

    @classmethod
    def as_dict(cls, location):
        result = {}
        tmp_dict = location._asdict()
        location_name = cls.get_location_name_by_id(tmp_dict['location_id'])
        if location_name:
            result['location_name'] = location_name
        if 'date' in tmp_dict:
            result['date'] = tmp_dict['date']
        if 'visits' in tmp_dict:
            result['visits'] = tmp_dict['visits']
        if 'total_visits' in tmp_dict:
            result['total_visits'] = tmp_dict['total_visits']
        return result

    @classmethod
    def convert_list_to_dicts(cls, location_stats):
        visits = []
        for location in location_stats:
            visits.append(AudienceLocationDate.as_dict(location))

        return visits


def maybe_negate(value, inputvalue, negate=False):
    if negate:
        return not_(value == inputvalue)
    return (value == inputvalue)


def init_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    log.info('Google analytics database tables are set-up')
