from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from pylons import config


def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials, cache_discovery=False)

    return service


# https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/service-py
def init_service(credentials_file):
    """
    Given a file containing the service accounts credentials
    will return a service object representing the analytics API.
    """

    # Define the auth scopes to request.
    scope = 'https://www.googleapis.com/auth/analytics.readonly'

    # Authenticate and construct service.
    service = get_service(
        api_name='analytics',
        api_version='v3',
        scopes=[scope],
        key_file_location=credentials_file)

    return service


def get_profile_id(service):
    """
    Get the profile ID for this user and the service specified by the
    'googleanalytics.id' configuration option. This function iterates
    over all of the accounts available to the user who invoked the
    service to find one where the account name matches (in case the
    user has several).
    """
    accounts = service.management().accounts().list().execute()

    if not accounts.get('items'):
        return None

    # These values need to be set in the .ini file
    # Name of analytics account
    accountName = config.get('googleanalytics.account')
    # Id of analytics property or app
    webPropertyId = config.get('googleanalytics.id')

    # Get id for analytics account based on the name
    for acc in accounts.get('items'):
        if acc.get('name') == accountName:
            accountId = acc.get('id')

    # Get all analytics views
    profiles = service.management().profiles().list(
        accountId=accountId, webPropertyId=webPropertyId).execute()

    # Return the first view id from analytics
    if profiles.get('items'):
        return profiles.get('items')[0].get('id')

    return None
