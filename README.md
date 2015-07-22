# BLPInterface

This is a wrapper around the Python Bloomberg API.  It is designed to roughly emulate the Excel API by implementing the traditional BDP, BDH and BDS requests.  All calls return consistantly formatted DataFrames.

This module functions in both Python 2.7 and 3.4 and requires Pandas and the blpapi library.  You can install the Python 2 version of the Bloomberg API from their webpage, or the Python 3 version of the Bloomberg API from github.

This module only implements the //BLP/refdata service and therefore does not handle subscriptions.  The following requests are implemented:

  - BDP: referenceRequest(securities, fields)
  - BDH: historicalRequest(securities, fields, startDate, endDate)
  - BDS: bulkRequest(securities, fields)

### Advantages
This implementation is clean and robust.  It checks requests for Security or Field exceptions and will raise as appropriate.  Additionally, it successfully handles bulk data requests and requests containing multiple securities and/or fields.

### Examples
```
# Single Security/Field
blp.referenceRequest('BBD/B CN Equity', 'GICS_SECTOR')

# Multiple Securities/Fields
blp.referenceRequest(['CNR CN Equity', 'CP CN Equity'], ['SECURITY_NAME_REALTIME', 'LAST_PRICE']))
```

```
# Basic Historical Request
blp.historicalRequest('BMO CN Equity', 'PX_LAST', '20141231', '20150131')

# Multiple Fields, Dates as datetime
blp.historicalRequest('BNS CN Equity', ['PX_LAST', 'PX_VOLUME'], datetime(2014, 12, 31), datetime(2015, 1, 31)))

# Multiple Securities/Fields
blp.historicalRequest(['CM CN Equity', 'NA CN Equity'], ['PX_LAST', 'PX_VOLUME'], '20141231', '20150131')

# Arbitrary keyword arguments are included in the request
blp.historicalRequest('TD CN Equity', 'PCT_CHG_INSIDER_HOLDINGS', '20141231', '20150131', periodicitySelection='WEEKLY')
```

```
# Bulk Data Requests
blp.bulkRequest('CP CN Equity','PG_REVENUE'))
```
A full set of examples can be found within the module.