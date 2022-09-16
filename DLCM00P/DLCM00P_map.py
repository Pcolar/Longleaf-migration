field_map = {
'C1CN': 'Name Id',
'C1REGN': 'REGISTERED BUSINESS NAME',
#'C1PIDN': 'PERSONAL IDENTITY',
'C1PCNM': 'REGISTERED BUSINESS NAME',
'C1PAD0': 'POSTAL ADDRESS 0',
'C1PAD1': 'POSTAL ADDRESS 1',
'C1PAD2': 'POSTAL ADDRESS 2',
'C1PAD3': 'POSTAL ADDRESS 3',
'C1PAD4': 'POSTAL ADDRESS 4',
'C1PAD5': 'POSTAL ADDRESS 5',
'C1PAD6': 'POSTAL ADDRESS 6',
#'C1PDLVN': 'PREFERRED DELIVERY NUMBER',
#'C1DXNO': 'DX NUMBER',
#'C1DXLOC': 'DX LOCATION',
#'C1PHN': 'TELEPHONE',
#'C1FAX': 'FAX',
'C1CSTS': 'Do Not Use Ind',
#'C1CONO': 'COMPANY CODE',
#'C1BR': 'BRANCH CODE',
'C1RGN': 'POSTAL ADDRESS 6',
#'C1SLRP': 'SALES REP CODE',
'C1CDIS': 'Name Class Id',
#'C1PRCD': 'PRICE CODE',
'C1XINV': 'Invoice Copies',
#'C1XNVF': 'XTRA INV NET VALUE FLG',
#'C1CTXC': 'CUSTOMER TAX CODE',
#'C1TXNO': 'CUSTOMER TAX EXEMPT NO.',
#'C1PYTC': 'DEBTORS PAYMENT TERMS CODE',
#'C1ILIC': 'IMPORT LICENCE NUMBER',
'C1SAN': 'SAN Number',
'C1BN': 'Default Order Bill To ID',
'C1CCLS': 'Credit Hold ID',
#'C1EXCD': 'CURRENCY EXCHANGE CODE',
#'C1CRLM': 'INTERNAL CREDIT LIMIT',
#'C1CIAC': 'EXTERNAL CREDIT LIMIT',
#'C1IBR': 'WAREHOUSE LOCATION',
#'C1WH': 'WAREHOUSE NUMBER',
#'C1CAR': 'CARRIER CODE',
#'C1FAGC': 'FORWARDING AGENT CODE',
#'C1RUN': 'DELIVERY RUN NUMBER',
#'C1DSTP': 'DELIVERY STOP NUMBER (WITHIN RUN)',
#'C1STTY': 'STATIONERY TYPE',
'C1CMNT': 'Credit Message',
#'C1OIF': 'OPEN ITEM ACCOUNT (Y/N)',
#'C1DLVP': 'ACCEPT PARTIAL DELIVERY BY LINE',
'C1DLOP': 'Partial Orders Ind',
'C1CBOA': 'Book Customer BO Cancel ID',
'C1OCON': 'Consolidate Orders Ind',
#'C1SBI': 'ACCEPT SUBSTITUTE',
#'C1ONRF': 'REQUIRE ORDER NUMBER',
#'C1CUTO': 'REQUIRE CUT-OFF DATE',
#'C1MIF': 'MINIMUM INVOICE',
'C1FSCF': 'Returnable Ind',
#'C1CIEC': 'CUSTOMER ITEM ENTRY CODE',
#'C1INCA': 'INTEREST CALCULATION (Y/N)',
#'C1DFCF': 'DUTY FREE CUSTOMER FLAG',
'C1AFCG': 'Calc Freight Ind',
#'C1CFRT': 'CHARGE FREIGHT ON CREDITS',
#'C1CL1': 'CUST LEVEL 1',
#'C1CL2': 'CUST LEVEL 2',
#'C1CL3': 'CUST LEVEL 3',
#'C1CL4': 'CUST LEVEL 4',
#'C1CL5': 'CUST LEVEL 5',
#'C1MJS': 'MAJOR STORE',
#'C1STN': 'STORE NO.',
#'C1BOCR': 'BACK ORDER CONFIRMATION CODE',
#'C1ECR': 'EDI CONFIRMATION CODE',
#'C1STMT': 'STATEMENT CODE',
#'C1RLSP': 'RELEASE PRIORITY',
'C1CTO': 'Carton Rounding Type',
#'C1CASC': 'CUSTOMER ALPHA SEARCH CODE',
#'C1REGZ': 'REGISTRATION TIMESTAMP',
'C1REGU': 'Created By User Id',
#'C1CHGZ': 'CHANGE TIMESTAMP',
'C1CHGU': 'Last Changed By User Id'}

C1MJS_acct_map = {
    '00106270': 'BNC',
    '00103869': 'B&T',
    '00101517': 'ENT',
    '00101528': 'EBS',
    '00108319': 'EVN',
    '00105713': 'FHE',
    '00111212': 'SMI',
    '00107346': 'TXB',
    '00100236': 'WNP',
    '00103586': 'BND',
    '00107871': 'BND',
    '00109543': 'BND',
    '00111564': 'BND' 
}
#C1CSTS_map = {
#    'X': 'A',
#    'Y': 'C',
#    'Z': 'S',
#    'H': 'A',
#    'I': 'A',
#    'J': 'A',
#    'N': 'C'
#}
C1CDIS_map = {
'AM': 'RDN',
'BI': 'RD',
'BN': 'RD',
'CBD': 'WD',
'CBR': 'BK',
'DS': 'WD',
'FL': 'ORN',
'FR': 'BK',
'FW': 'WD',
'LC': 'ORN',
'LP': 'ORN',
'LS': 'ORN',
'OR': 'ORN',
'RD': 'RD',
'RDS': 'BK',
'RG': 'NB',
'RGC': 'NB',
'RI': 'BK',
'RM': 'RDN',
'RN': 'RD',
'RO': 'BK',
'RS': 'BK',
'RU': 'BK',
'WR': 'WD',
'WRG': 'WD',
'WS': 'WD',
'WT': 'WD'
}
C1PYTC_map = {
'0': '0',
'00': '0',
'10': '30',
'15': '30',
'20': '20',
'30': '30',
'45': '60',
'60': '60',
'90': '90',
'120': '12',
'150': '30',
'180': '18',
'CONS': 'CS',
'DED': 'DD',
'G': 'G',
'P': 'PP',
'PRO': 'PP',
'REF': 'RF',
'SPC': '30'
}
C1CBOA_map = {
'1': 'A',
'2': 'A',
'3': 'A',
'4': 'A',
'5': 'A',
'6': 'A',
'7': 'A',
'8': 'A',
'9': 'A',
'10': '1',
'11': '1',
'12': '1',
'13': 'A',
'NOBO': 'N'
}
C1CCLS_map = {
'B': 'B',
'BC': 'BC',
'C': 'C',
'FRD': 'FRD',
'GR': 'GR',
'H': 'H',
'L': 'L',
'M': 'M',
'NEW': 'PP',
'P': 'PP',
'PP': 'PP',
'PRO': 'PP',
'TAX': 'PP',
'VHOL': 'PP',
'Z': 'Z'
}