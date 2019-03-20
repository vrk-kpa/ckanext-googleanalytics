from ckanext.googleanalytics.model import PackageStats
from ckan.plugins import toolkit
from ckan.model import Package


@toolkit.side_effect_free
def googleanalytics_dataset_visits(context=None, data_dict=None):
    """
    Fetch the amount of times a dataset hs been visited

    :param id: Dataset id
    :type id: string

    :returns: The number of times the dataset has been viewed
    :rtype: integer
    """
    package = Package.get(data_dict['id'])
    return PackageStats.get_all_visits(package.id)
