from ckan.common import OrderedDict
from ckanext.googleanalytics.model import PackageStats, ResourceStats, AudienceLocationDate
from datetime import datetime, timedelta


def google_analytics_dataset_report(time):
    '''
    Generates report based on google analytics data. number of views per package
    '''
    
    today = datetime.today()
    if time == 'week':
        start_date = today - timedelta(days=today.weekday(), weeks=1)
        end_date = today - timedelta(days=today.weekday() + 1)
    elif time == 'month':
        end_date = today.replace(day=1) - timedelta(days=1)
        start_date = end_date.replace(day=1)
    elif time == 'year':
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=365)
        

    # get package objects corresponding to popular GA content
    top_packages = PackageStats.get_total_visits(start_date=start_date, end_date=end_date, limit=0)
    top_20 = top_packages[:20]


    return {
        'table': top_packages,
        'top': top_20
    }


def google_analytics_dataset_option_combinations():
    options = ['week', 'month', 'year']
    for option in options:
        yield {'time': option}


googleanalytics_dataset_report_info = {
    'name': 'google-analytics-dataset',
    'title': 'Most popular datasets',
    'description': 'Google analytics showing top datasets with most views',
    'option_defaults': OrderedDict((('time', 'month'),)),
    'option_combinations': google_analytics_dataset_option_combinations,
    'generate': google_analytics_dataset_report,
    'template': 'report/dataset_analytics.html',
}

def google_analytics_dataset_least_popular_report(time):
    '''
    Generates report based on google analytics data. number of views per package
    '''
    
    today = datetime.today()
    if time == 'week':
        start_date = today - timedelta(days=today.weekday(), weeks=1)
        end_date = today - timedelta(days=today.weekday() + 1)
    elif time == 'month':
        end_date = today.replace(day=1) - timedelta(days=1)
        start_date = end_date.replace(day=1)
    elif time == 'year':
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=365)
        

    # get package objects corresponding to popular GA content
    top_packages = PackageStats.get_total_visits(start_date=start_date, end_date=end_date, limit=0, desc=False)
    top_20 = top_packages[:20]

    return {
        'table': top_packages,
        'top': top_20
    }


def google_analytics_dataset_least_popular_option_combinations():
    options = ['week', 'month', 'year']
    for option in options:
        yield {'time': option}


googleanalytics_dataset_least_popular_report_info = {
    'name': 'google-analytics-dataset-least-popular',
    'title': 'Least popular datasets',
    'description': 'Google analytics showing top datasets with least views',
    'option_defaults': OrderedDict((('time', 'month'),)),
    'option_combinations': google_analytics_dataset_least_popular_option_combinations,
    'generate': google_analytics_dataset_least_popular_report,
    'template': 'report/dataset_analytics.html',
}


def google_analytics_resource_report(last):
    '''
    Generates report based on google analytics data. number of views per package
    '''
    # get resource objects corresponding to popular GA content
    top_resources = ResourceStats.get_top(limit=last)

    return {
        'table': top_resources.get("resources")
    }


def google_analytics_resource_option_combinations():
    options = [20, 25, 30, 35, 40, 45, 50]
    for option in options:
        yield {'last': option}


googleanalytics_resource_report_info = {
    'name': 'google-analytics-resource',
    'title': 'Most popular resources',
    'description': 'Google analytics showing most downloaded resources',
    'option_defaults': OrderedDict((('last', 20),)),
    'option_combinations': google_analytics_resource_option_combinations,
    'generate': google_analytics_resource_report,
    'template': 'report/resource_analytics.html'
}


def google_analytics_location_report():
    '''
    Generates report based on google analytics data. number of sessions per location
    '''

    top_locations = AudienceLocationDate.get_total_top_locations(20)

    last_month_end = datetime.today().replace(day=1) - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    finland_vs_world_last_month = AudienceLocationDate.special_total_location_to_rest(last_month_start, last_month_end,
                                                                                      'Finland')

    finland_vs_world_all = AudienceLocationDate.special_total_location_to_rest(datetime(2000, 1, 1), datetime.today(),
                                                                               'Finland')

    sessions_by_month = AudienceLocationDate.special_total_by_months()

    data_for_export = AudienceLocationDate.special_total_by_months(datetime(2000, 1, 1), last_month_end)

    # first item in table list will be available for export
    return {
        'table': data_for_export,
        'data': {
            'top_locations': top_locations,
            'finland_vs_world_last_month': finland_vs_world_last_month,
            'finland_vs_world_all': finland_vs_world_all,
            'sessions_by_month': sessions_by_month,
        }
    }


googleanalytics_location_report_info = {
    'name': 'google-analytics-location',
    'title': 'Audience locations',
    'description': 'Google analytics showing most audience locations (bot traffic is filtered out)',
    'option_defaults': None,
    'option_combinations': None,
    'generate': google_analytics_location_report,
    'template': 'report/location_analytics.html'
}


def google_analytics_organizations_with_most_popular_datasets(time):
    most_popular_organizations = PackageStats.get_organizations_with_most_popular_datasets(time)
    return {
        'table': most_popular_organizations
    }


googleanalytics_organizations_with_most_popular_datasets_info = {
    'name': 'google-analytics-most-popular-organizations',
    'title': 'Most popular organizations',
    'description': 'Google analytics showing most popular organizations by visited datasets',
    'option_defaults': OrderedDict((('time', 'month'),)),
    'option_combinations': google_analytics_dataset_option_combinations,
    'generate': google_analytics_organizations_with_most_popular_datasets,
    'template': 'report/organization_analytics.html'
}
