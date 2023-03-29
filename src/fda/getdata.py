import requests
import pandas as pd
import time
import json
import os


def get_sec_companies(email):

    '''
    
    This function retrieves all companies which are currently listed at the Securities and Exchange Comission

    Parameters:
    --------
    email: str
        an email which identifies the user to the API request

    Returns:
    ---------
    pd.DataFrame

    Example:
    >>> sec_companies = get_sec_companies("firsname.lastname@organization.com")

    '''

    headers = {'User-Agent': 'email'}

    url = 'https://www.sec.gov/files/company_tickers.json'
    r = requests.get(url, headers=headers)
    tickers_cik = pd.json_normalize(pd.json_normalize(r.json(), max_level=0).values[0])
    tickers_cik['cik'] = tickers_cik['cik_str'].astype(str).str.zfill(10)
    tickers_cik.columns = ['cik', 'ticker', 'title', 'cik_long']
    
    return tickers_cik.loc[:, ['ticker', 'title', 'cik', 'cik_long']]


def get_sec_company_information(sec_companies, submission_dir):

    '''

    This function prepares company information (cik, entityType, sic, sicDescription, name, tickers, exchanges, fiscalYearEnd) from sec submission files.
    It assumes that you previously downloaded submission files from the SEC which can be found here under bulk data: https://www.sec.gov/edgar/sec-api-documentation
    Note that for companies with multiple tickers, only the first occuring ticker is used.

    Parameters:
    ------------
    sec_companies: pd.DataFrame
        a data frame which should be generated with the get_sec_companies function
    
    submission_dir: str
        the data path where unzipped submission files are stored

    Returns:
    ---------
    pd.DataFrame

    '''

    get_keys = [
        'cik',
        'entityType',
        'sic',
        'sicDescription',
        'name',
        'tickers',
        'exchanges',
        'fiscalYearEnd'
    ]

    ciks_with_multiple_tickers = sec_companies[sec_companies.cik_long.duplicated()].cik_long.values
    unique_cik_companies = sec_companies[sec_companies.cik_long.duplicated()==False]

    started = False
    no_info = 0
    for idx, row in unique_cik_companies.iterrows():
        
        cik = row['cik_long']
        with open(os.path.join(submission_dir, f'CIK{cik}.json')) as f:
            r = json.load(f)

        company_info_tmp = pd.DataFrame(dict([(key, value) for (key, value) in zip(r.keys(), r.values()) if key in get_keys]))
        if company_info_tmp.shape[0] == 0:
            no_info += 1
            continue
        elif cik in ciks_with_multiple_tickers:
            company_info_tmp.loc[:, 'has_multiple_symbols'] = True
        else:
            company_info_tmp.loc[:, 'has_multiple_symbols'] = False

        if started:
            company_info = pd.concat((company_info, company_info_tmp.iloc[0].to_frame().transpose()))
        else:
            company_info = company_info_tmp.iloc[0].to_frame().transpose().copy()
            started = True

    company_info.reset_index(drop = True, inplace = True)
    print(f'Company information for {no_info} cik numbers could not be retrieved.')

    return company_info


def get_seccompany_filings(cik, submission_dir = None, email = None):

    assert isinstance(cik, str) and len(cik) == 10, 'cik must be a 10 digit identifier in string format'

    if not(submission_dir) and not(email):
        raise ValueError('Either submission_dir or headers must be specified.')
    
    
    '''
    
    This function lists all SEC filings for a company. You can use previously downloaded submission files from the SEC which can be found here under bulk data: https://www.sec.gov/edgar/sec-api-documentation
    or directly request them from the SEC api if you provide your email address.

    Parameters:
    ------------
    cik: str
        string with ten digits representing the SEC identifier cik number
    
    submission_dir: str
        the data path where unzipped submission files are stored

    email: str
        an email which declares the user to the SEC

    Returns:
    ---------
    pd.DataFrame

    '''

    if submission_dir:
        with open(os.path.join(submission_dir, f'CIK{cik}.json')) as f:
            r = json.load(f)

        filings = pd.DataFrame(r['filings']['recent'])

        if len(r['filings']['files']) > 0:
            for file in r['filings']['files']:
                with open(os.path.join(submission_dir, file['name'])) as f:
                    further_filings = pd.DataFrame(json.load(f))
                filings = pd.concat((filings, further_filings))
    else:           
        headers = {'User-Agent': email}
        url = f'https://data.sec.gov/submissions/CIK{cik}.json'
        r = requests.get(url, headers = headers)
        filings = pd.DataFrame(r.json()['filings']['recent'])

        for extra_filings in r.json()['filings']['files']:
            url_tmp = f'https://data.sec.gov/submissions/' + extra_filings['name']
            r_tmp = requests.get(url_tmp, headers = headers)
            filings = pd.concat((filings, pd.DataFrame(r_tmp.json())), axis = 0)
            time.sleep(.1)
        

    filings.sort_values(by = 'acceptanceDateTime', inplace = True)
    filings.reset_index(drop = True, inplace = True)
    
    return filings


def get_fmp_stock_data(apikey, ticker, start_date = None, end_date = None):

    '''
    A functiono to download historical stock market data using the financial modeling prep api (https://site.financialmodelingprep.com/developer/docs/).

    Parameters:
    ------------
    apikey: str
        the fmp api key

    ticker: str
        the symbol which is used to identify stocks at stock market places

    start_date (optional): str (YYYY-MM-DD)
        the start date

    end_date (optional): str (YYYY-MM-DD)
        the end date of the time series

    Returns:
    ---------
    pd.DataFrame
        the data frame contains historical open, high, low, close, adjusted close prices and trading volumne
    '''
    
    url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}'

    if end_date and not(start_date):
        raise ValueError('Please provide a start_date when providing an end_date.')

    parameters = {'apikey': apikey}
    if start_date:
        parameters['from'] = start_date
    if end_date:
        parameters['to'] = end_date

    r = requests.get(url, params = parameters)

    ts = pd.DataFrame(r.json()['historical'])
    ts.sort_values(by = 'date', inplace = True)
    ts.reset_index(drop = True, inplace = True)
    return ts.loc[:, ['open', 'high', 'low', 'close', 'adjClose', 'volume']]