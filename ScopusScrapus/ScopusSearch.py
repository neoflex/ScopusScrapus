import requests as r
import urllib.parse as purl
import json


def StartScopusSearch(key, params):
    ssq = ScopusSearchQuery(key, params)
    return ssq


class ScopusSearchQuery:
    _defaultParams = {'count': 100,
                      'view': 'COMPLETE',
                      'httpAccept': 'application/json'}
    _baseUrl = "http://api.elsevier.com/content/search/scopus?"
    _support_pagination = True
    _root_key = 'search-results'

    def __init__(self, key, params, timeout=60, apikey_return=False):
        self._apiKey = key
        self._keys = None
        if isinstance(key, list):
            self._keys = key
            self._keyCount = 0
            self._apiKey = key[0]
        self._state = "empty"
        self._params = params
        self._data = []
        self._nextUrl = None
        self._i = 0
        self._count = 0
        self._timeout = timeout
        self._apikey_return = apikey_return  # Return (self._data[self._i-1], self._apiKey) instead of self._data[self._i-1]

    def _make_search_url(self):
        params = self._params
        defParams = self._defaultParams
        pSet = set(params.keys()).union(set(defParams.keys()))
        parameters = {key: params[key] if key in params else defParams[key] for key in pSet}

        querystring = purl.urlencode(parameters)
        apiKeyString = purl.urlencode({'apiKey': self._apiKey})
        url = "{}{}{}{}".format(self._baseUrl, querystring, '&', apiKeyString)
        return url

    def _manageQuotaExcess(self, raiseOnQE=False):
        print("Managing quota exess...")
        if raiseOnQE or self._keys is None:
            raise Exception("QuotaExceeded - You must wait 7 days until quotas are reset")
        self._nextUrl = None
        self._keyCount = self._keyCount + 1
        print("Key was: "+self._apiKey)
        try:
            self._apiKey = self._keys[self._keyCount]
            print("Key is: "+self._apiKey)
            return self._run_search()
        except IndexError:
            return self._run_search(True)  # If we fail again, we surrender

    def _run_search(self, raiseOnQE=False):

        url = self._nextUrl
        if url is None: url = self._make_search_url()
        if url == "done": raise StopIteration()

        qRes = r.get(url,timeout=self._timeout)
        if qRes.status_code in [429, 401]:
            # If Invalid API Key or exceeding quota for the API Key, change it
            return self._manageQuotaExcess(raiseOnQE)
        dta = qRes.json()

        if qRes.status_code != 200:
            raise Exception("{} {} {} {}".format("Error: ",
                                                 dta['service-error']['status']['statusText'],
                                                 "URL is:", url)) # Fix this

        # KeyError hazard: If no 'next' url is available, we need to error out anyway
        nxtLink = [ln for ln in dta[self._root_key]['link'] if ln['@ref'] == 'next']

        if len(nxtLink) > 0: self._nextUrl = nxtLink[0]['@href']
        else: self._nextUrl = "done" # Nasty? Sorry : )
        return dta[self._root_key]['entry']  # Returning only the obtained results

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self._i == len(self._data):
            self._data = self._run_search()
            self._i = 0
        self._i += 1
        if self._apikey_return:
            return self._data[self._i-1], self._apiKey
        elif not self.apikey_return:
            return self._data[self._i-1]


class ScopusSerialTitle(ScopusSearchQuery):
    
    _defaultParams = {'count': 100,
                      'view': 'CITESCORE',
                      'httpAccept': 'application/json'}
    _baseUrl = "http://api.elsevier.com/content/serial/title"
    _support_pagination = False
    _root_key = 'serial-metadata-response'

    def _make_search_url(self):

        params = self._params
        defParams = self._defaultParams
        pSet = set(params.keys()).union(set(defParams.keys()))
        parameters = {key: params[key] if key in params else defParams[key] for key in pSet}
        if "issn" in parameters:
            base_url = self._baseUrl + '/issn/' + parameters['issn'] + '?'
            if 'title' in parameters:
                parameters.pop('title')
            parameters.pop('issn')
        else:
            base_url = self._baseUrl + '?'
        querystring = purl.urlencode(parameters)
        apiKeyString = purl.urlencode({'apiKey': self._apiKey})
        url = "{}{}{}{}".format(self._baseUrl, querystring, '&', apiKeyString)
        return url

class SerialTitleQuery:

    # Serial title search
    _defaultParams = {'count': 200,
                      'view': 'CITESCORE',
                      'httpAccept': 'application/json'}
    _baseUrl = "http://api.elsevier.com/content/serial/title?"
    _support_pagination = False
    _root_key = 'serial-metadata-response'

    def __init__(self, key, params, timeout=60):
        self._apiKey = key
        self._keys = None
        if isinstance(key, list):
            self._keys = key
            self._keyCount = 0
            self._apiKey = key[0]
        self._state = "empty"
        self._params = params
        self._data = []
        self._nextUrl = None
        self._i = 0
        self._count = 0
        self._timeout = timeout

    def _make_search_url(self):
        params = self._params
        pset = set(params.keys()).union(set(self._defaultParams.keys()))
        parameters = {key: params[key] if key in params else self._defaultParams[key] for key in pset}

        # make query
        querystring = purl.urlencode(parameters)
        apiKeyString = purl.urlencode({'apiKey': self._apiKey})
        url = "{}{}{}{}".format(self._base_url, querystring, '&', apiKeyString)
        return url

    def _manageQuotaExcess(self,
                           raiseOnQE=False):

        print("Managing quota exess...")
        if raiseOnQE or self._keys is None:
            raise Exception("QuotaExceeded - You must wait 7 days until quotas are reset")
        self._nextUrl = None
        self._keyCount = self._keyCount + 1
        print("Key was: " + self._apiKey)
        try:
            self._apiKey = self._keys[self._keyCount]
            print("Key is: " + self._apiKey)
            return self._run_search()
        except IndexError:
            return self._run_search(True)  # If we fail again, we surrender

    def _run_search(self, raiseOnQE=False):

        url = self._nextUrl
        if url is None:
            url = self._make_search_url()
        if url == "done":
            raise StopIteration()

        qRes = r.get(url, timeout=self._timeout)
        if qRes.status_code in [429, 401]:
            # If Invalid API Key or exceeding quota for the API Key, change it
            return self._manageQuotaExcess(raiseOnQE)

        dta = qRes.json()
        if qRes.status_code != 200:
            logger.info("{} {} {} {}".format("Error: ",
                                             dta['service-error']['status']['statusText'], "URL is:", url))
            raise StopIteration()

        if 'entry' not in dta[self._root_key]:
            raise StopIteration()

        if _defaultParams['count'] <= len(dta[self._root_key]['entry']):
            next_url = dta[self._root_key]['link'][0]['@href']
            parsed = urlparse.urlparse(next_url)
            parsed_query = urlparse.parse_qs(parsed.query)
            start = int(parsed_query['start'][0]) + int(parsed_query['count'][0])
            self._nextUrl = "{0}&{1}={2}".format(self._make_search_url(), 'start', start)
            if start >= 10000:
                self._nextUrl = "done"

        else:
            self._nextUrl = "done"
        return dta[self._root_key]['entry']

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):

        if self._i == len(self._data):
            self._data = self._run_search()
            self._i = 0
        if len(self._data) == self._i:
            pass  # Raise error
        self._i += 1
        return self._data[self._i - 1]
