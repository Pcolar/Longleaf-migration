field_map = {
'A1CN': 'Record Key',
'A1STS': 'Pay Status',
'A1CNM': 'Vendor Name',
'A1APCC': 'Vendor Type',
'A1CAD0': 'Address Line 1',
'A1CAD1': 'Address Line 2',
'A1CAD2': 'Address Line 3',
'A1CAD3': 'City',
'A1CAD4': 'State',
'A1CAD5': 'Zip',
'A1CAD6': 'Country',
'A1PYTC': 'Terms Code',
'A1PNM': 'Payee',
'A1EMLA': 'Email Address',
'A1HOLD': 'Pay Status',
'A1REGZ': 'Account Opened Date',
'A1CHGZ': 'Last Changed'
}

A1STS_map = {
    'P': 'A',
    'H': 'A',
    'S': 'C'
}

A1APCC_map = {
    'FW': 'FW',
    'WC': 'REG',
    '1ACH': 'REG',
    '2ACH': 'ROY',
    '1': 'REG',
    '2': 'ROY',
    '3': 'BEN',
    '5': 'AF',
    '6': 'REG'    
}

A1PYTC_map = {
    '00': '00',
    '0': '00',
    '10': '10',
    '15': '15', # check with Jen
    '20': '15',
    '30': '30',
    '45': '45',
    '60': '60',
    '90': '90',
    '110': '11', # check with Jen
    '120': '12',
    'CONS': '30',
    'N15': '15',
    'N30': '30'    
}
