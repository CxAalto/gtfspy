from pyaml import yaml
import requests

def get_source_meta_data(site=None, data_sources_fname="data.yaml"):
    with open(data_sources_fname, 'r') as f:
        data_sources = yaml.load(f)['sites']
    if site:
        return data_sources[site]
    else:
        return data_sources

def get_credential_data(site=None,
                        credentials_fname="../../credentials-transit/credentials.yaml"):
    """
    Load the credentials.yaml file
    """
    with open(credentials_fname, 'r') as f:
        data_credentials = yaml.load(f)['sites']
    if site:
        if data_credentials.has_key(site):
            return data_credentials[site]
        else:
            return {}
    else:
        return data_credentials

def assert_authorization_integrity():
    """
    Check that authorization information in data.yaml
    and credentials.yaml match.
    """
    data_sources = get_source_meta_data()
    data_credentials = get_credential_data()

    # assert authentication
    for (site, locdata) in data_sources.iteritems():
        if locdata['authentication']:
            assert data_credentials.has_key(site)


def download_gtfs(site, outfname):
    """
    Download gtfs data for a given country/city.
    """
    sources = get_source_meta_data(site)
    credentials = get_credential_data(site)
    gtfs_url = sources['files']['gtfs']['url']

    auth = {}
    auth_keys = ['password', 'username', 'api_key']
    for key in auth_keys:
        if credentials.has_key(key):
            auth[key] = credentials[key]
    download_file(gtfs_url, outfname, auth)


def download_file(url, outfname, auth):
    """
    To load a file from url to local location outfname using
    authorization information provided.
    (This code was originally adapted from:
    http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py)

    Paramaters
    ----------
    url : str
        url to the gtfs location
    outfname : str
        path to the location where the file should be stored
    auth: dict
        dictionary containing information on the authentication type
    """
    if not auth:
        # auth is a empty dict -> no authentication required
        r = requests.get(url, stream=True)
    elif auth.has_key('api_key'):
        # api-key authentication
        api_key = auth['api_key']
        r = requests.get(url, params={'key': api_key}, stream=True)
    elif auth.has_key('username'):
        # username, passwd authentication:
        user = auth['username']
        passwd = auth['password']
        r = requests.get(url, stream=True, auth=(user, passwd))

    print r.url
    r.raise_for_status()

    with open(outfname, 'wb') as f:
        print "downloading " + url
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)



if __name__ == "__main__":
    assert_authorization_integrity()
    # these should all work
    # download_gtfs('Finland', "/tmp/test.zip")
    # download_gtfs('Helsinki', "/tmp/test.zip")
    # download_gtfs('Sweden', "/tmp/test.zip")
    # download_gtfs('Oulu', "/tmp/test.zip")
    # download_gtfs('Tampere', "/tmp/test.zip")



