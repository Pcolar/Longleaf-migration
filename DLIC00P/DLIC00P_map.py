field_map = {
'Product_ID': 'Product ID',
'ISBN13': 'ISBN13',
'Company_No': 'Company No',
'Minor_Disc_ID': 'Minor Disc ID',
'Minor_Discipline_Description': 'Minor Discipline Description',
'BISAC1': 'BISAC1',
'BISAC2': 'BISAC2',
'BISAC3': 'BISAC3',
'Type_ID': 'Type ID',
'Title': 'Title',
'Publication_Status_ID': 'Publication Status ID',
'Series_ID': 'Series ID',
'Season': 'Season',
'Editor': 'Editor',
'First_Due_Date': 'First Due Date',
'Interest_Code': 'Interest Code',
'Rights_Code': 'Rights Code',
'Rights_Areas': 'Rights Areas',
'Can_Distribute_Countries_Country_ID': 'Can Distribute Countries Country ID',
'Can_Distribute_Countries_Country_Name': 'Can Distribute Countries Country Name',
'Can_Distribute_Countries_Assoc_Country_Group': 'Can Distribute Countries Assoc Country Group',
'Cannot_Distribute_Countries_Assoc_Country_Group': 'Cannot Distribute Countries Assoc Country Group',
'Cannot_Distribute_Countries_Country_ID': 'Cannot Distribute Countries Country ID',
'Cannot_Distribute_Countries_Country_Name': 'Cannot Distribute Countries Country Name'
}

class_aggregator_map = ['Product_ID','ISBN13','Company_No','Minor_Disc_ID','Minor_Discipline_Description','BISAC1','BISAC2','BISAC3','Type_ID','Title','Publication_Status_ID','Series_ID','Season','Editor','First_Due_Date','Interest_Code','Rights_Code','Rights_Areas','Can_Distribute_Countries_Country_ID','Can_Distribute_Countries_Country_Name','Can_Distribute_Countries_Assoc_Country_Group','Cannot_Distribute_Countries_Assoc_Country_Group','Cannot_Distribute_Countries_Country_ID','Cannot_Distribute_Countries_Country_Name']
# create index for above
Product_ID = 0
ISBN13 = 1
Company_No = 2
Minor_Disc_ID = 3
Minor_Discipline_Description = 4
BISAC1 = 5
BISAC2 = 6
BISAC3 = 7
Type_ID = 8
Title = 9
Publication_Status_ID = 10
Series_ID = 11
Season = 12
Editor = 13
First_Due_Date = 14
Interest_Code = 15
Rights_Code = 16
Rights_Areas = 17
Can_Distribute_Countries_Country_ID = 18
Can_Distribute_Countries_Country_Name = 19
Can_Distribute_Countries_Assoc_Country_Group = 20
Cannot_Distribute_Countries_Assoc_Country_Group = 21
Cannot_Distribute_Countries_Country_ID = 22
Cannot_Distribute_Countries_Country_Name = 23

ic_subm_map = {
'RURALHIST': 'RURALHIS',
'SABR': 'SABR',
'SCIENCE': 'SCIENCE',
'SCIENCEBIO': 'SCIENCEB',
'SCIENCENAT': 'SCIENCEN',
'SCIFI': 'SCIFI',
'SEAS': 'SEAS',
'SECR': 'SECR',
'SEUS': 'SEUS',
'SHORTSTOR': 'SHORTSTO',
'SLAVERY': 'SLAVERY',
'SLAVICST': 'SLAVICST',
'SLFHLP': 'SLFHLP',
'SOCIOLOGY': 'SOCIOLOG',
'SOCMED': 'SOCMED',
'SOUTHERN': 'SOUTHERN',
'SOUTHHIST': 'SOUTHHIS',
'SOUTHLIT': 'SOUTHLIT',
'SOUTHWEST': 'SOUTHWES',
'SOVIETEAST': 'SOVIETEA',
'SPAIN': 'SPAIN',
'SPCFLGHT': 'SPCFLGHT',
'SPECIALED': 'SPECIALE',
'SPORTHIST': 'SPORTHIS',
'SPORTS': 'SPORTS',
'SPRTLTY': 'SPRTLTY',
'STNBLTY': 'STNBLTY',
'SWUS': 'SWUS',
'TECHNOLOGY': 'TECHNOLO',
'TEXAS': 'TEXAS',
'THEO': 'THEO',
'TRANSLAT': 'TRANSLAT',
'TRANSPORT': 'TRANSPOR',
'TRAVEL': 'TRAVEL',
'TRUECRIME': 'TRUECRIM',
'TV': 'TV',
'UGAU': 'UGAU',
'UNMU': 'UNMU',
'URBAN': 'URBAN',
'USDIST': 'USDIST',
'UTOPIANISM': 'UTOPIANI',
'VAHIST': 'VAHIST',
'VANATURE': 'VANATURE',
'VAREGD': 'VAREGD',
'VASPRT': 'VASPRT',
'VTNM': 'VTNM',
'WEATHER': 'WEATHER',
'WESTHIST': 'WESTHIST',
'WNAFF': 'WNAFF',
'WOMEN': 'WOMEN',
'WOMENHIST': 'WOMENHIS',
'WOODSCI': 'WOODSCI',
'WORLDHIST': 'WORLDHIS',
'WRITING': 'WRITING',
'WSTRN': 'WSTRN',
'WW1': 'WW1',
'WWI': 'WWI',
'WWII': 'WWII',
'YLLWSTN': 'YLLWSTN',
'YNGRDRS': 'YNGRDRS'
}
ic_int_map = {
'ARTCRAFT': 'ARTCRAFT',
'FASHION': 'FASHION',
'ARCHITECT': 'ARCHITEC',
'ARTARCH': 'ARTARCH',
'LANDSCAPE': 'LANDSCAP',
'PRESERV': 'PRESERV',
'ART': 'ART',
'ARTCONTEM': 'ARTCONTE',
'BIBLE': 'BIBLE',
'BIBSTU': 'BIBSTU',
'BIOGRAPHY': 'BIOGRAPH',
'MEMOIRS': 'MEMOIRS',
'BUSINESS': 'BUSINESS',
'ADVERT': 'ADVERT',
'ECONHIST': 'ECONHIST',
'STATISTICS': 'STATISTI',
'ECON': 'ECON',
'GLOBAL': 'GLOBAL',
'COMIC': 'COMIC',
'COOKING': 'COOKING',
'TECHNOLOGY': 'TECHNOLO',
'ARTCRAFT': 'ARTCRAFT',
'DECOART': 'DECOART',
'DRAMA': 'DRAMA',
'DRAMAFR': 'DRAMAFR',
'EDUCATION': 'EDUCATIO',
'CHILDHOOD': 'CHILDHOO',
'SPECIALED': 'SPECIALE',
'STATISTICS': 'STATISTI',
'FAMILYREL': 'FAMILYRE',
'AMLIT': 'AMLIT',
'FICTION': 'FICTION',
'SCIFI': 'SCIFI',
'SHORTSTOR': 'SHORTSTO',
'WSTRN': 'WSTRN',
'AFAMLIT': 'AFAMLIT',
'AMASIALIT': 'AMASIALI',
'FORLAN': 'FORLAN',
'GAMES': 'GAMES',
'GARDEN': 'GARDEN',
'HEALTH': 'HEALTH',
'NUTRI': 'NUTRI',
'HIST': 'HIST',
'AFRICAN': 'AFRICAN',
'CLASSICS': 'CLASSICS',
'ASIANHIST': 'ASIANHIS',
'BRITHIST': 'BRITHIST',
'HISTORY': 'HISTORY',
'IRISHHIST': 'IRISHHIS',
'MILHIST': 'MILHIST',
'AMHIST': 'AMHIST',
'COLONIAL': 'COLONIAL',
'CIVILWAR': 'CIVILWAR',
'AMERWEST': 'AMERWEST',
'CARHIST': 'CARHIST',
'HOLOCAUST': 'HOLOCAUS',
'POLAR': 'POLAR',
'MARITIME': 'MARITIME',
'PRECOLUM': 'PRECOLUM',
'HUMOR': 'HUMOR',
'JUVENILE': 'JUVENILE',
'JUVENILE': 'JUVENILE',
'LANGUAGE': 'LANGUAGE',
'COMMUNICAT': 'COMMUNIC',
'WRITING': 'WRITING',
'JOURNALISM': 'JOURNALI',
'LING': 'LING',
'LAW': 'LAW',
'CIVILRGT': 'CIVILRGT',
'CRIMJUST': 'CRIMJUST',
'GOVT': 'GOVT',
'GOVT': 'GOVT',
'GOVT': 'GOVT',
'LITCOLL': 'LITCOLL',
'CLASSLIT': 'CLASSLIT',
'CARAFLIT': 'CARAFLIT',
'BRITLIT': 'BRITLIT',
'LITESSAYS': 'LITESSAY',
'LITCRIT': 'LITCRIT',
'AFROCRIT': 'AFROCRIT',
'AFAMCRIT': 'AFAMCRIT',
'CARIBCRIT': 'CARIBCRI',
'BRITCRIT': 'BRITCRIT',
'CLASSCRIT': 'CLASSCRI',
'JEWISHLIT': 'JEWISHLI',
'LITTHEORY': 'LITTHEOR',
'ASIACRIT': 'ASIACRIT',
'COMPLIT': 'COMPLIT',
'MATH': 'MATH',
'LOGIC': 'LOGIC',
'STATISTICS': 'STATISTI',
'MEDICINE': 'MEDICINE',
'DENTAL': 'DENTAL',
'HISTMED': 'HISTMED',
'MEDETHICS': 'MEDETHIC',
'PHARMA': 'PHARMA',
'MUSIC': 'MUSIC',
'MUSIC-COUN': 'MUSIC-CO',
'NATURE': 'NATURE',
'BIRDING': 'BIRDING',
'NATCON': 'NATCON',
'HORSES': 'HORSES',
'MAMM': 'MAMM',
'MUSHROOMS': 'MUSHROOM',
'NATWRT': 'NATWRT',
'WEATHER': 'WEATHER',
'GENERAL': 'GENERAL',
'SPRTLTY': 'SPRTLTY',
'PERFORM': 'PERFORM',
'FILM': 'FILM',
'SCREEN': 'SCREEN',
'TV': 'TV',
'PETS': 'PETS',
'DOGS': 'DOGS',
'PHILOSOPH': 'PHILOSOP',
'PHILOSPH': 'PHILOSPH',
'CLASSSTUD': 'CLASSSTU',
'ETHICS': 'ETHICS',
'PHOTO': 'PHOTO',
'POETRY': 'POETRY',
'AMERPOEM': 'AMERPOEM',
'POEBRI': 'POEBRI',
'POEAF': 'POEAF',
'POEAFR': 'POEAFR',
'POECR': 'POECR',
'POLISCI': 'POLISCI',
'GOVT': 'GOVT',
'POLITICS': 'POLITICS',
'POLTHRY': 'POLTHRY',
'DIPLOHIST': 'DIPLOHIS',
'LABOR': 'LABOR',
'GOVT': 'GOVT',
'GOVT': 'GOVT',
'PEACE': 'PEACE',
'HUMAN': 'HUMAN',
'GOVT': 'GOVT',
'UTOPIANISM': 'UTOPIANI',
'PSYCH': 'PSYCH',
'REFERENCE': 'REFERENC',
'BIBLIOG': 'BIBLIOG',
'GENEALOGY': 'GENEALOG',
'RELIGION': 'RELIGION',
'BIBSTU': 'BIBSTU',
'CHURCH': 'CHURCH',
'CHRISTIAN': 'CHRISTIA',
'ETHICS': 'ETHICS',
'ISLAM': 'ISLAM',
'JEWISHHIST': 'JEWISHHI',
'JEWISHLAW': 'JEWISHLA',
'CHRISTIAN': 'CHRISTIA',
'CHRISTIAN': 'CHRISTIA',
'SCIENCE': 'SCIENCE',
'BOTANY': 'BOTANY',
'ECOLOGY': 'ECOLOGY',
'ENERGY': 'ENERGY',
'ENTM': 'ENTM',
'GEOGR': 'GEOGR',
'EARTHPHYS': 'EARTHPHY',
'HISTSCI': 'HISTSCI',
'PHYSICS': 'PHYSICS',
'HERP': 'HERP',
'MYCOLOGY': 'MYCOLOGY',
'SPCFLGHT': 'SPCFLGHT',
'SELF-HELP': 'SELF-HEL',
'AGING': 'AGING',
'SOCIOLOGY': 'SOCIOLOG',
'AFAM': 'AFAM',
'ANTHRO': 'ANTHRO',
'CULSTUDY': 'CULSTUDY',
'ARCHAEOL': 'ARCHAEOL',
'CRIMINOLOG': 'CRIMINOL',
'ETHNIC': 'ETHNIC',
'FOLKLORE': 'FOLKLORE',
'GAYSTUDIES': 'GAYSTUDI',
'MEN': 'MEN',
'AMINDIAN': 'AMINDIAN',
'POPCULTURE': 'POPCULTU',
'SOCWRK': 'SOCWRK',
'RURALHIST': 'RURALHIS',
'URBAN': 'URBAN',
'STATISTICS': 'STATISTI',
'WOMEN': 'WOMEN',
'DISABILITY': 'DISABILI',
'GENDER': 'GENDER',
'AMERASIAN': 'AMERASIA',
'HISP': 'HISP',
'JEWISH': 'JEWISH',
'MEDIA': 'MEDIA',
'SLAVERY': 'SLAVERY',
'AGRI': 'AGRI',
'INDIGSTUD': 'INDIGSTU',
'SPORTS': 'SPORTS',
'BASEB': 'BASEB',
'BASKETB': 'BASKETB',
'BOXING': 'BOXING',
'CYCLING': 'CYCLING',
'FOOTBL': 'FOOTBL',
'GOLF': 'GOLF',
'SPORTHIST': 'SPORTHIS',
'HOCKEY': 'HOCKEY',
'HRSRCG': 'HRSRCG',
'RUN': 'RUN',
'SOCCER': 'SOCCER',
'TENNIS': 'TENNIS',
'TECHNOLOGY': 'TECHNOLO',
'ENGR': 'ENGR',
'CARTATLAS': 'CARTATLA',
'TRANSPORT': 'TRANSPOR',
'AVIATION': 'AVIATION',
'TRAVEL': 'TRAVEL',
'YA': 'YA'
}
ic_ser_map = {
'ELAN Series ID':'SERIES ID (A8)',
'EE':'EE',
'OIEA':'OIEA',
'NCRL':'NCRL',
'CWA':'CWA',
'9CSSA':'9CSSA',
'9CSPE':'9CSPE',
'18OCAI':'18OCAI',
'SLS':'SLS',
'18OCC1':'18OCC1',
'CWS':'CWS',
'IRS':'IRS',
'GAC':'GAC',
'6JSB':'6JSB',
'10NWS':'10NWS',
'JPP':'JPP',
'JHFS':'JHFS',
'9CPHCW':'9CPHCW',
'21SRL':'21SRL',
'CIM':'CIM',
'SBS':'SBS',
'8GJST':'8GJST',
'MEL':'MEL',
'10VVS':'10VVS',
'JTL':'JTL',
'NYS':'NYS',
'8FOC':'8FOC',
'6AML':'6AML',
'SMP':'SMP',
'18OAIL':'18OAIL',
'9ACI':'9ACI',
'UNCGLS':'UNCGLS',
'9USW':'9USW',
'16DS':'16DS',
'10JA':'10JA',
'16SARA':'16SARA',
'10USN':'10USN',
'EC':'EC',
'21DHAB':'21DHAB',
'6CSA':'6CSA',
'6MEX':'6MEX',
'SPC':'SPC',
'16MBC':'16MBC',
'6FON':'6FON',
'21CES':'21CES',
'NCWH':'NCWH',
'SLH':'SLH',
'9INDO':'9INDO',
'MIH':'MIH',
'14ND31':'14ND31',
'9CSCP':'9CSCP',
'6APB':'6APB',
'6FOS':'6FOS',
'6SWS':'6SWS',
'SSM':'SSM',
'ICMN':'ICMN',
'9WEA':'9WEA',
'GCP':'GCP',
'IRO':'IRO',
'18OOCC':'18OOCC',
'10CAR':'10CAR',
'9CSM':'9CSM',
'14SAC':'14SAC',
'01DSB':'01DSB',
'8BTB':'8BTB',
'MMS':'MMS',
'YSF':'YSF',
'21CCS':'21CCS',
'18OWFL':'18OWFL',
'10CWH':'10CWH',
'9NIUSV':'9NIUSV',
'6EF':'6EF',
'TVS':'TVS',
'LAT':'LAT',
'01LC':'01LC',
'6OO':'6OO',
'18ONDN':'18ONDN',
'6BOR':'6BOR',
'14ND25':'14ND25',
'AAA':'AAA',
'10SRC':'10SRC',
'9EXP':'9EXP',
'8SSIA':'8SSIA',
'9RWW':'9RWW',
'SPO':'SPO',
'6PW':'6PW',
'10LPP':'10LPP',
'NESC':'NESC',
'21FS':'21FS',
'10CFF':'10CFF',
'6SAN':'6SAN',
'18OCMR':'18OCMR',
'SG':'SG',
'18ORCA':'18ORCA',
'8GOHL':'8GOHL',
'WFL':'WFL',
'08WFNB':'08WFNB',
'6EMC':'6EMC',
'8NSS':'8NSS',
'16REC':'16REC',
'9SIG':'9SIG',
'6BFI':'6BFI',
'9CSAS':'9CSAS',
'9MP':'9MP',
'6MP':'6MP',
'6MW':'6MW',
'10CD':'10CD',
'ELLS':'ELLS',
'8PC20':'8PC20',
'8RAW':'8RAW',
'CSAS':'CSAS',
'9AGPE':'9AGPE',
'9CRPMP':'9CRPMP',
'9WHS':'9WHS',
'MJH':'MJH',
'LHCW':'LHCW',
'STSC':'STSC',
'18OCCV':'18OCCV',
'8SW':'8SW',
'15VLAP':'15VLAP',
'8LLS':'8LLS',
'06BCE':'06BCE',
'14CISW':'14CISW',
'8EAP':'8EAP',
'8AWP':'8AWP',
'8SLHS':'8SLHS',
'FME':'FME',
'20GEM':'20GEM',
'20PH':'20PH',
'6NVN':'6NVN',
'21SRL':'21SRL',
'15VHI':'15VHI',
'CI':'CI',
'8GOHL':'8GOHL',
'6ISE':'6ISE',
'18OAET':'18OAET',
'LSC':'LSC',
'9SIG':'9SIG',
'6ACN':'6ACN',
'6AIL':'6AIL',
'6FF':'6FF',
'8CP':'8CP',
'CH':'CH',
'MPA':'MPA',
'SHGR':'SHGR',
'18OOWB':'18OOWB',
'RTH':'RTH',
'CSUS':'CSUS',
'10CGW':'10CGW',
'NBH':'NBH',
'SUSC':'SUSC',
'10EMG':'10EMG',
'6PSF':'6PSF',
'10AMS':'10AMS',
'10EAH':'10EAH',
'NDSS':'NDSS',
'21SSJS':'21SSJS',
'01HCS':'01HCS',
'9ZZHULO':'9ZZHULO',
'6NH':'6NH',
'14ND61':'14ND61',
'8EHAS':'8EHAS',
'6ATB':'6ATB',
'6PSP':'6PSP',
'18ORDL':'18ORDL',
'SPS':'SPS',
'9CSHP':'9CSHP',
'6CGR':'6CGR',
'REL':'REL',
'10ALI':'10ALI',
'VOS':'VOS',
'21JRAL':'21JRAL',
'9ZZAMP1':'9ZZAMP1',
'6WGE':'6WGE',
'10SAH':'10SAH',
'01RTP':'01RTP',
'8CRUX':'8CRUX',
'DAC':'DAC',
'9CSPR':'9CSPR',
'8UNCW':'8UNCW',
'BLCW':'BLCW',
'16SARA':'16SARA',
'LMJ':'LMJ',
'14ND35':'14ND35',
'6OSF':'6OSF',
'8US&A':'8US&A',
'LHHS':'LHHS',
'20WMFB':'20WMFB',
'6CLJ':'6CLJ',
'6MEX':'6MEX',
'18OWCS':'18OWCS',
'GG':'GG',
'BPS':'BPS',
'6SJC':'6SJC',
'6BYA':'6BYA',
'21CES':'21CES',
'9LERA':'9LERA',
'21SAA':'21SAA',
'6FF':'6FF',
'9AMIS':'9AMIS',
'9CSOL':'9CSOL',
'9ZZKSR':'9ZZKSR',
'MEB':'MEB',
'14ND15':'14ND15',
'8WTS':'8WTS',
'16QUE':'16QUE',
'6AIL':'6AIL',
'6CSS':'6CSS',
'6ST':'6ST',
'18OCLT':'18OCLT',
'14ND6':'14ND6',
'10PAG':'10PAG',
'10WRV':'10WRV',
'10WWC':'10WWC',
'MCCW':'MCCW',
'21CILA':'21CILA',
'6DGP':'6DGP',
'9CUSHWA':'9CUSHWA',
'9ZZLGS':'9ZZLGS',
'9ZZSHL':'9ZZSHL',
'18OEMA':'18OEMA',
'AAW':'AAW',
'14ND42':'14ND42',
'10REP':'10REP',
'17SRTD':'17SRTD',
'6INE':'6INE',
'16PPA':'16PPA',
'09NIUSEEES':'09NIUSEE',
'6JEJ':'6JEJ',
'6HAA':'6HAA',
'6PRO':'6PRO',
'14ND60':'14ND60',
'10MCSP':'10MCSP',
'SRC':'SRC',
'16SARR':'16SARR',
'6TBP':'6TBP',
'20ALJ':'20ALJ',
'9ZZAMP2':'9ZZAMP2',
'9ZZMEDLO':'9ZZMEDLO',
'6JPSH':'6JPSH',
'18OAPM':'18OAPM',
'UTO':'UTO',
'1914-1459':'1914-145',
'14CLMS':'14CLMS',
'14NDLP':'14NDLP',
'08FOA':'08FOA',
'9CIILRR':'9CIILRR',
'9ZT':'9ZT',
'01RTP':'01RTP',
'9CON':'9CON',
'14ND65':'14ND65',
'15CMS':'15CMS',
'08CGS':'08CGS',
'PTF':'PTF',
'6HAR':'6HAR',
'6WW':'6WW',
'18OACH':'18OACH',
'6AF':'6AF',
'1925-5888':'1925-588',
'8WFS':'8WFS',
'20MJH':'20MJH',
'21CCP':'21CCP',
'9CORJ':'9CORJ',
'9PWS':'9PWS',
'6JAJT':'6JAJT',
'6SMS':'6SMS',
'6SYM':'6SYM',
'6TC':'6TC',
'6TED':'6TED',
'6FON':'6FON',
'14ND45':'14ND45',
'8GHSL':'8GHSL',
'01NCT':'01NCT',
'6EWW':'6EWW',
'21CCS':'21CCS',
'NAW':'NAW',
'NYL':'NYL',
'14ND62':'14ND62',
'14ND73':'14ND73',
'10WPR':'10WPR',
'81970':'81970',
'DAV':'DAV',
'9BCSMH':'9BCSMH',
'9CPHCW':'9CPHCW',
'6SA':'6SA',
'6SPG':'6SPG',
'18AFMS':'18AFMS',
'18OOSS':'18OOSS',
'14ND57':'14ND57',
'8FF':'8FF',
'8SMHF':'8SMHF',
'HOS':'HOS',
'9CSILR':'9CSILR',
'20CSA':'20CSA',
'6AML':'6AML',
'9DASH':'9DASH',
'9MSRC':'9MSRC',
'9ZZORPI':'9ZZORPI',
'6LNA':'6LNA',
'6MTB':'6MTB',
'6SPG':'6SPG',
'6ME':'6ME',
'14CM':'14CM',
'8NPCW':'8NPCW',
'8SFA':'8SFA',
'8STS':'8STS',
'16HAF':'16HAF',
'21CSPN':'21CSPN',
'6ATB':'6ATB',
'9IS':'9IS',
'14ND32':'14ND32',
'10BUS':'10BUS',
'10WEA':'10WEA',
'15VIHE':'15VIHE',
'15VV':'15VV',
'NWGS':'NWGS',
'16SARP':'16SARP',
'9BDM':'9BDM',
'9NIUSG':'9NIUSG',
'6AML':'6AML',
'17BHGNT':'17BHGNT',
'9CWP':'9CWP',
'9ZZFOTU':'9ZZFOTU',
'6JSD':'6JSD',
'6IW':'6IW',
'6NLA':'6NLA',
'6ATB':'6ATB',
'6EWW':'6EWW',
'6LNA':'6LNA',
'6OSF':'6OSF',
'18OANH':'18OANH',
'18OBGS':'18OBGS',
'18OISA':'18OISA',
'18OPLH':'18OPLH',
'6SF':'6SF',
'14ND14':'14ND14',
'08RAW':'08RAW',
'13LGBB':'13LGBB',
'8ALS':'8ALS',
'8VQR':'8VQR',
'10BNP':'10BNP',
'16SWA':'16SWA',
'9FWP':'9FWP',
'20SAW':'20SAW',
'6BUR':'6BUR',
'9CY':'9CY',
'9MLCR':'9MLCR',
'9NHPW':'9NHPW',
'6SWS':'6SWS',
'18AATS':'18AATS',
'18OWLS':'18OWLS',
'6WA':'6WA',
'ATC':'ATC',
'14ND26':'14ND26',
'10FI':'10FI',
'10RICH':'10RICH',
'08GJST':'08GJST',
'8NPS':'8NPS',
'SR':'SR',
'THC':'THC',
'10CSM':'10CSM',
'16RAS':'16RAS',
'16WOW':'16WOW',
'20VAW':'20VAW',
'9HAE':'9HAE',
'6JBC':'6JBC',
'6SNL':'6SNL',
'6FOS':'6FOS',
'18ONDT':'18ONDT',
'CAA':'CAA',
'SSG':'SSG',
'14ND47':'14ND47',
'10MIDC':'10MIDC',
'10PBSH':'10PBSH',
'08SSIA':'08SSIA',
'FOC':'FOC',
'16RIV':'16RIV',
'03CBS':'03CBS',
'9CTW':'9CTW',
'9SIGT':'9SIGT',
'18AKWS':'18AKWS',
'6WW':'6WW',
'6RTP':'6RTP',
'17SWC':'17SWC',
'9SSHS':'9SSHS',
'6BPP':'6BPP',
'6GCC':'6GCC',
'6SJS':'6SJS',
'6SPW':'6SPW',
'6WCS':'6WCS',
'6CSA':'6CSA',
'6CSS':'6CSS',
'MBL':'MBL',
'WAW':'WAW',
'14ND27':'14ND27',
'14ND5':'14ND5',
'08S1970':'08S1970',
'08UWS':'08UWS',
'8SAS':'8SAS',
'LHHJS':'LHHJS',
'RHJS':'RHJS',
'6OL':'6OL',
'9AES':'9AES',
'9CIILRR':'9CIILRR',
'9FSAC':'9FSAC',
'6JCV':'6JCV',
'6HAW':'6HAW',
'6LAW':'6LAW',
'ASL':'ASL',
'LJP':'LJP',
'RG':'RG',
'14ND50':'14ND50',
'10CAP':'10CAP',
'15VPLA':'15VPLA',
'08GRB':'08GRB',
'8SOS':'8SOS',
'8SRE':'8SRE',
'RAL':'RAL',
'WRL':'WRL',
'21HBBC':'21HBBC',
'21ILH':'21ILH',
'9PST':'9PST',
'9ZZCWOI':'9ZZCWOI',
'6EWW':'6EWW',
'6ISE':'6ISE',
'6TW':'6TW',
'WCCP':'WCCP',
'1928-1722':'1928-172',
'10MSS':'10MSS',
'8LOP':'8LOP',
'JSS':'JSS',
'16ALA':'16ALA',
'9EPI':'9EPI',
'20AMER':'20AMER',
'9NIUOX':'9NIUOX',
'9NIUSE':'9NIUSE',
'18AWFS':'18AWFS',
'6SAN':'6SAN',
'6NS':'6NS',
'6PSP':'6PSP',
'6WBC':'6WBC',
'17MSSEC':'17MSSEC',
'9CSCH':'9CSCH',
'9HCT':'9HCT',
'9MPII':'9MPII',
'6CB':'6CB',
'6JDI':'6JDI',
'6JTC':'6JTC',
'6NAI':'6NAI',
'6AML':'6AML',
'6ELA':'6ELA',
'6RRS':'6RRS',
'6TC':'6TC',
'18OLAW':'18OLAW',
'14ND40':'14ND40',
'10CWJ':'10CWJ',
'10MLI':'10MLI',
'10MPR':'10MPR',
'08AVAW':'08AVAW',
'08EHAS':'08EHAS',
'10CSM':'10CSM',
'8SL':'8SL',
'16SARI':'16SARI',
'16HAW':'16HAW',
'16JLA':'16JLA',
'01CRNC':'01CRNC',
'9BLG':'9BLG',
'20HEL':'20HEL',
'18AFMS':'18AFMS',
'6FF':'6FF',
'6WBC':'6WBC',
'9CENT':'9CENT',
'9CW':'9CW',
'9DWC':'9DWC',
'9MESS':'9MESS',
'9MLL':'9MLL',
'9RS':'9RS',
'9SMR':'9SMR',
'9ZZKART':'9ZZKART',
'9ZZSTPE':'9ZZSTPE',
'6JG':'6JG',
'6CHH':'6CHH',
'6ELA':'6ELA',
'6REA':'6REA',
'6SSC':'6SSC',
'18AKWS':'18AKWS',
'18ONNL':'18ONNL',
'CPD':'CPD',
'07BB':'07BB',
'19PTR':'19PTR',
'14ND23':'14ND23',
'14ND54':'14ND54',
'14POL':'14POL',
'10VBS':'10VBS',
'10WCL':'10WCL',
'15ICFAM':'15ICFAM',
'08PCS':'08PCS',
'08PCTS':'08PCTS',
'4NHCLS':'4NHCLS',
'8ESHS':'8ESHS',
'8SLS':'8SLS',
'8SVP':'8SVP',
'BARL':'BARL',
'EBS':'EBS',
'ECS':'ECS',
'16SARH':'16SARH',
'16HORN':'16HORN',
'20PAC':'20PAC',
'18AATS':'18AATS',
'6THG':'6THG',
'09BCSMH':'09BCSMH',
'9LP':'9LP',
'9ZZJFL':'9ZZJFL',
'9ZZSTECG':'9ZZSTECG',
'6AIL':'6AIL',
'6RTP':'6RTP',
'6ST':'6ST',
'18OOWS':'18OOWS',
'18OVC':'18OVC',
'6GW':'6GW',
'6HW':'6HW',
'NND':'NND',
'14ND20':'14ND20',
'14ND58':'14ND58',
'14ND8':'14ND8',
'10JAY':'10JAY',
'10STS':'10STS',
'10WAS':'10WAS',
'10JT':'10JT',
'10SAH':'10SAH',
'8GPP':'8GPP',
'8KC':'8KC',
'AAW':'AAW',
'GIP':'GIP',
'MEL':'MEL',
'MIH':'MIH',
'10STU':'10STU',
'16HDF':'16HDF',
'08EAP':'08EAP',
'9CCH':'9CCH',
'9ACI':'9ACI',
'09NIUOX':'09NIUOX',
'18ANPR':'18ANPR',
'6BRT':'6BRT',
'6OSF':'6OSF',
'AMB':'AMB',
'9BDM':'9BDM',
'9CHM':'9CHM',
'9CSEN':'9CSEN',
'9NYHJ':'9NYHJ',
'9CMIP':'9CMIP',
'9ZZADF':'9ZZADF',
'6CON':'6CON',
'6HSL':'6HSL',
'6JWC':'6JWC',
'6LWW':'6LWW',
'6MAW':'6MAW',
'6OL':'6OL',
'6SHA':'6SHA',
'6SPWS':'6SPWS',
'6APB':'6APB',
'6LAW':'6LAW',
'6SAN':'6SAN',
'6SYM':'6SYM',
'6WW':'6WW',
'6AC':'6AC',
'6SIH':'6SIH',
'0829-755X':'0829-755',
'14ND18':'14ND18',
'14ND59':'14ND59',
'14ND9':'14ND9',
'14NDMAP':'14NDMAP',
'10RA':'10RA',
'10SPS':'10SPS',
'15VIMA':'15VIMA',
'04LT':'04LT',
'10SAHC':'10SAHC',
'8CC':'8CC',
'8CHAU':'8CHAU',
'8MAS':'8MAS',
'HTA':'HTA',
'TST':'TST',
'10MHSC':'10MHSC',
'16ML':'16ML',
'16ZIA':'16ZIA',
'08WFNB':'08WFNB',
'01CRS':'01CRS',
'9NIUSV':'9NIUSV',
'9XRSSS':'9XRSSS',
'20MBMT':'20MBMT',
'21SPP':'21SPP',
'18AMMS':'18AMMS',
'18AWLW':'18AWLW',
'6FML':'6FML',
'6SAI':'6SAI',
'6PSF':'6PSF',
'9CESIPP':'9CESIPP',
'9CHS':'9CHS',
'9NIUSV':'9NIUSV',
'9RAPL':'9RAPL',
'9TLS':'9TLS',
'9ZZLOPS':'9ZZLOPS',
'9ZZSAS':'9ZZSAS',
'6CJY':'6CJY',
'6EEC':'6EEC',
'6CHS':'6CHS',
'6PSP':'6PSP',
'6PW':'6PW',
'18AWLW':'18AWLW',
'14ND10':'14ND10',
'14ND33':'14ND33',
'15BLL':'15BLL',
'15TLHN':'15TLHN',
'8GAS':'8GAS',
'FF':'FF',
'WLAC':'WLAC',
'10LCP':'10LCP',
'16CC':'16CC',
'16GRS':'16GRS',
'16LM':'16LM',
'08GRB':'08GRB',
'08GRNG':'08GRNG',
'08MCKC':'08MCKC',
'9CENT':'9CENT',
'9CSPE':'9CSPE',
'9RWW':'9RWW',
'20TSH':'20TSH',
'9NIUP':'9NIUP',
'9YDSS':'9YDSS',
'6BFI':'6BFI',
'6OL':'6OL',
'09NNIS':'09NNIS',
'9AGRL':'9AGRL',
'9CFR':'9CFR',
'9ZZAAL':'9ZZAAL',
'9ZZCII':'9ZZCII',
'9ZZPLU':'9ZZPLU',
'6FJ':'6FJ',
'6EH':'6EH',
'6FWR':'6FWR',
'6JSI':'6JSI',
'6LE':'6LE',
'6BOR':'6BOR',
'6NLA':'6NLA',
'18OALP':'18OALP',
'18OCSS':'18OCSS',
'18OLHN':'18OLHN',
'18OWAW':'18OWAW',
'6AP':'6AP',
'6NDU':'6NDU',
'21SSJS':'21SSJS',
'MED':'MED',
'NYH':'NYH',
'19HRSJC':'19HRSJC',
'2291-9627':'2291-962',
'14ND12':'14ND12',
'14ND34':'14ND34',
'14ND48':'14ND48',
'14ND53':'14ND53',
'10CRF':'10CRF',
'10JFL':'10JFL',
'10PBK':'10PBK',
'10REM':'10REM',
'10WCF':'10WCF',
'08MUL':'08MUL',
'8CBN':'8CBN',
'8GRBS':'8GRBS',
'8JLE':'8JLE',
'CIM':'CIM',
'DLTM':'DLTM',
'GCP':'GCP',
'GG':'GG',
'IRS':'IRS',
'JTL':'JTL',
'16WBS':'16WBS',
'08RAW':'08RAW',
'9LAL':'9LAL',
'9NFA':'9NFA',
'10LAAB':'10LAAB',
'20CON':'20CON',
'01CULS':'01CULS',
'UNCGLS':'UNCGLS',
'6BAW':'6BAW',
'6LWW':'6LWW',
'17BHHB':'17BHHB',
'9CSILR':'9CSILR',
'9CTW':'9CTW',
'9HARR':'9HARR',
'9OLANA':'9OLANA',
'9PIRSA':'9PIRSA',
'9ZZCEM':'9ZZCEM',
'9ZZVLOU':'9ZZVLOU',
'6HRI':'6HRI',
'6NS':'6NS',
'6RRN':'6RRN',
'6EH':'6EH',
'6IW':'6IW',
'6JSI':'6JSI',
'6RRN':'6RRN',
'6EB':'6EB',
'6ND':'6ND',
'05VWA':'05VWA',
'NYC':'NYC',
'0709-2997':'0709-299',
'14ND1':'14ND1',
'14ND37':'14ND37',
'14ND39':'14ND39',
'14ND41':'14ND41',
'14ND56':'14ND56',
'14ND63':'14ND63',
'14ND66':'14ND66',
'14NDRIREC':'14NDRIRE',
'10AGE':'10AGE',
'10GWB':'10GWB',
'01WRL':'01WRL',
'08AWWP':'08AWWP',
'08NSS':'08NSS',
'08SLS':'08SLS',
'10MHSC':'10MHSC',
'8MMLD':'8MMLD',
'ATC':'ATC',
'BARLA':'BARLA',
'LEP':'LEP',
'LMJ':'LMJ',
'REL':'REL',
'SPC':'SPC',
'16APLS':'16APLS',
'16CB':'16CB',
'16CON':'16CON',
'16VAR':'16VAR',
'08FOA':'08FOA',
'08GPP':'08GPP',
'08NPS':'08NPS',
'08WFP':'08WFP',
'01TTYR':'01TTYR',
'9CLAS':'9CLAS',
'9ZTAM':'9ZTAM',
'9AMP':'9AMP',
'20DVAN':'20DVAN',
'20FMM':'20FMM',
'20MJLC':'20MJLC',
'20MSA':'20MSA',
'20WGW':'20WGW',
'9ASBS':'9ASBS',
'9NJH':'9NJH',
'21NCHE':'21NCHE',
'21SJC':'21SJC',
'08EAP':'08EAP',
'01SLA':'01SLA',
'6EXW':'6EXW',
'6GPP':'6GPP',
'6HAW':'6HAW',
'6LAW':'6LAW',
'6REA':'6REA',
'6ATB':'6ATB',
'6ISE':'6ISE',
'17BHS':'17BHS',
'17SRR':'17SRR',
'9CC':'9CC',
'9CCH':'9CCH',
'9CSAB':'9CSAB',
'9CSIA':'9CSIA',
'9EASL':'9EASL',
'9RC':'9RC',
'9SIGT':'9SIGT',
'9UNU':'9UNU',
'9ZZDRR':'9ZZDRR',
'9ZZEPM':'9ZZEPM',
'9ZZSCS':'9ZZSCS',
'9ZZTIT':'9ZZTIT',
'9ZZVLET':'9ZZVLET',
'6BE':'6BE',
'6JDR':'6JDR',
'06APL':'06APL',
'6ALL':'6ALL',
'6BFI':'6BFI',
'6CA':'6CA',
'6CII':'6CII',
'6RRS':'6RRS',
'6RTP':'6RTP',
'6SAS':'6SAS',
'6THG':'6THG',
'6BFI':'6BFI',
'6GCC':'6GCC',
'6IF':'6IF',
'6INE':'6INE',
'6LAS':'6LAS',
'6LWW':'6LWW',
'6PSF':'6PSF',
'6RCS':'6RCS',
'6TED':'6TED',
'6THG':'6THG',
'NYS':'NYS',
'18AECC':'18AECC',
'18OPVN':'18OPVN',
'18OSSC':'18OSSC',
'AMB':'AMB',
'SWS':'SWS',
'14BBP':'14BBP',
'14DNCAT':'14DNCAT',
'14ND28':'14ND28',
'14ND36':'14ND36',
'14ND44':'14ND44',
'14ND67':'14ND67',
'14ND81':'14ND81',
'14NDKIS':'14NDKIS',
'10KDB':'10KDB',
'10RIC':'10RIC',
'10WRT':'10WRT',
'15VCMC':'15VCMC',
'01BS':'01BS',
'08EAP':'08EAP',
'08HIH':'08HIH',
'10PAR':'10PAR',
'8CYW':'8CYW',
'BRAS':'BRAS',
'MEB':'MEB',
'MED':'MED',
'NYS':'NYS',
'PAM':'PAM',
'RTH':'RTH',
'SPO':'SPO',
'SPS':'SPS',
'TVS':'TVS',
'10PAR':'10PAR',
'08NSS':'08NSS',
'08S1970':'08S1970',
'8GAS':'8GAS',
'8MAS':'8MAS',
'9WCS':'9WCS',
'9ZTCRG':'9ZTCRG',
'9CSSA':'9CSSA',
'9YDSS':'9YDSS',
'10LAMB':'10LAMB',
'9PFOT':'9PFOT',
'9WEA':'9WEA',
'21PUHRS':'21PUHRS',
'18ANHS':'18ANHS',
'6GCC':'6GCC',
'6ISE':'6ISE',
'6NS':'6NS',
'6OO':'6OO',
'6SWS':'6SWS',
'6TC':'6TC',
'6WW':'6WW',
'17DEAC':'17DEAC',
'NYS':'NYS',
'11M':'11M',
'9AMW':'9AMW',
'9CSCL':'9CSCL',
'9GFBL':'9GFBL',
'9ILRB':'9ILRB',
'9NNI':'9NNI',
'9PGS':'9PGS',
'9ROED':'9ROED',
'9WWCP':'9WWCP',
'9ZZAMP3':'9ZZAMP3',
'9ZZBLL':'9ZZBLL',
'9ZZBLN':'9ZZBLN',
'9ZZHER':'9ZZHER',
'9ZZSOCT':'9ZZSOCT',
'9ZZSSEH':'9ZZSSEH',
'9ZZSTAN':'9ZZSTAN',
'9ZZSYMA':'9ZZSYMA',
'9ZZVECON':'9ZZVECON',
'6AJT':'6AJT',
'6KC':'6KC',
'6BPPHM':'6BPPHM',
'6CYF':'6CYF',
'6ECCS':'6ECCS',
'6FML':'6FML',
'6IF':'6IF',
'6JMP':'6JMP',
'6MGC':'6MGC',
'6SAI':'6SAI',
'6BYA':'6BYA',
'6FML':'6FML',
'6NAI':'6NAI',
'6NS':'6NS',
'6OO':'6OO',
'6WCS':'6WCS',
'18AMWS':'18AMWS',
'18OAPA':'18OAPA',
'18OCCS':'18OCCS',
'18ORPS':'18ORPS',
'6NDUF':'6NDUF',
'NY':'NY',
'14LISCC':'14LISCC',
'14ND11':'14ND11',
'14ND46':'14ND46',
'14ND52':'14ND52',
'14ND68':'14ND68',
'14ND69':'14ND69',
'10AMSP':'10AMSP',
'10MNS':'10MNS',
'10MRT':'10MRT',
'10TTTB':'10TTTB',
'10VSCS':'10VSCS',
'15VCLAS':'15VCLAS',
'15VJA':'15VJA',
'03CBS':'03CBS',
'04INOC':'04INOC',
'08CCPP':'08CCPP',
'08NPS':'08NPS',
'08SFA':'08SFA',
'15VHI':'15VHI',
'8BTBS':'8BTBS',
'8GHC':'8GHC',
'8SWSP':'8SWSP',
'JCMLS':'JCMLS',
'MOD':'MOD',
'RIHS':'RIHS',
'UNCCH-AD':'UNCCH-AD',
'URPDS':'URPDS',
'10BUS':'10BUS',
'10MHS':'10MHS',
'10UM':'10UM',
'16PPA':'16PPA',
'16ALAWT':'16ALAWT',
'16COR':'16COR',
'16LLMSFS':'16LLMSFS',
'16RPB':'16RPB',
'16SARI':'16SARI',
'16SARP':'16SARP',
'16SARR':'16SARR',
'16WEP':'16WEP',
'03SCS':'03SCS',
'08MUL':'08MUL',
'8SAR':'8SAR',
'01PDSR':'01PDSR',
'01RMNC':'01RMNC',
'9KI':'9KI',
'9NHM':'9NHM',
'9IS':'9IS',
'9MP':'9MP',
'20AT':'20AT',
'20DH':'20DH',
'20EE':'20EE',
'20IHP':'20IHP',
'20LHRP':'20LHRP',
'20WS':'20WS',
'9INDO':'9INDO',
'21AA':'21AA',
'UNCCH-AD':'UNCCH-AD',
'DLTM':'DLTM',
'06BMM':'06BMM',
'06TIP':'06TIP',
'6AFSA':'6AFSA',
'18ABGS':'18ABGS',
'18AECC':'18AECC',
'AMB':'AMB',
'6ALL':'6ALL',
'6FOS':'6FOS',
'6NAI':'6NAI',
'6NLA':'6NLA',
'6PW':'6PW',
'6FOS':'6FOS',
'6HAR':'6HAR',
'6IF':'6IF',
'6LAW':'6LAW',
'6SWS':'6SWS',
'17PS':'17PS',
'MEL':'MEL',
'11SLSI':'11SLSI',
'9CFSS':'9CFSS',
'9CPS':'9CPS',
'9CSEE':'9CSEE',
'9ECE':'9ECE',
'9FWP':'9FWP',
'9HRW':'9HRW',
'9SIH':'9SIH',
'9WEA':'9WEA',
'9ZZAAM':'9ZZAAM',
'9ZZCIV':'9ZZCIV',
'9ZZLIP':'9ZZLIP',
'9ZZLLS':'9ZZLLS',
'9ZZLOCS':'9ZZLOCS',
'9ZZORCMS':'9ZZORCMS',
'9ZZSYMD':'9ZZSYMD',
'9ZZUPL':'9ZZUPL',
'9ZZVWET':'9ZZVWET',
'6SAI':'6SAI',
'IRS':'IRS',
'6AT':'6AT',
'6ATR':'6ATR',
'6FBP':'6FBP',
'6WBC':'6WBC',
'6WIG':'6WIG',
'6AT':'6AT',
'6CHH':'6CHH',
'6CII':'6CII',
'6CYF':'6CYF',
'6FWR':'6FWR',
'6HAR':'6HAR',
'6HRI':'6HRI',
'6MSL':'6MSL',
'6REA':'6REA',
'18AWFS':'18AWFS',
'18GCC':'18GCC',
'6COMH':'6COMH',
'6MC':'6MC',
'21CCP':'21CCP',
'7BB':'7BB',
'7BSPS':'7BSPS',
'7CIH':'7CIH',
'14ND17':'14ND17',
'14ND3':'14ND3',
'14ND49':'14ND49',
'14ND7':'14ND7',
'14SSBE':'14SSBE',
'10CSM':'10CSM',
'10DBS':'10DBS',
'10ERP':'10ERP',
'10GMP':'10GMP',
'10KL':'10KL',
'10VVS':'10VVS',
'04MODLAN':'04MODLAN',
'08GRNG':'08GRNG',
'10MHS':'10MHS',
'10NWS':'10NWS',
'10VVS':'10VVS',
'10WP':'10WP',
'15ICFAM':'15ICFAM',
'17LEC':'17LEC',
'17MSSEC':'17MSSEC',
'6SASP':'6SASP',
'6SJC':'6SJC',
'8CBAS':'8CBAS',
'8HSE':'8HSE',
'8RBR':'8RBR',
'8UGLM':'8UGLM',
'8UGM':'8UGM',
'8WNS':'8WNS',
'CPD':'CPD',
'DS':'DS',
'FWMS':'FWMS',
'IRSS':'IRSS',
'JCWE':'JCWE',
'LAN':'LAN',
'LPC':'LPC',
'MJH':'MJH',
'OIEAHC':'OIEAHC',
'OSS':'OSS',
'PEG':'PEG',
'RLS':'RLS',
'SOG':'SOG',
'SVPWW':'SVPWW',
'URPD':'URPD',
'WAW':'WAW',
'10EAH':'10EAH',
'10CSM':'10CSM',
'10AVP':'10AVP',
'10MHS':'10MHS',
'10SAHC':'10SAHC',
'10EXHI':'10EXHI',
'10DE':'10DE',
'10JA':'10JA',
'10EMP':'10EMP',
'16MHTG':'16MHTG',
'16SARC':'16SARC',
'10JAY':'10JAY',
'16NAW':'16NAW',
'16SIA':'16SIA',
'IC':'IC',
'8DAP':'8DAP',
'8RAW':'8RAW',
'01PJI':'01PJI',
'01PWAG':'01PWAG',
'01PWWH':'01PWWH',
'01PZBV':'01PZBV',
'9ILRB':'9ILRB',
'9CPH':'9CPH',
'9CSAB':'9CSAB',
'9CSEE':'9CSEE',
'9MESS':'9MESS',
'9CON':'9CON',
'9ROED':'9ROED',
'9WHS':'9WHS',
'01PPEA':'01PPEA',
'6BPP':'6BPP',
'9INDO':'9INDO',
'20JKB':'20JKB',
'20MSAL':'20MSAL',
'9NIUSB':'9NIUSB',
'21CHCF':'21CHCF',
'18ACWW':'18ACWW',
'18AHSC':'18AHSC',
'18ASWS':'18ASWS',
'6CHH':'6CHH',
'6CII':'6CII',
'6CSS':'6CSS',
'6INE':'6INE',
'6IW':'6IW',
'6JMP':'6JMP',
'6JWC':'6JWC',
'6PSP':'6PSP',
'6RTP':'6RTP',
'6ST':'6ST',
'6WCS':'6WCS',
'6EWW':'6EWW',
'6LWW':'6LWW',
'6MTB':'6MTB',
'17CEHL':'17CEHL',
'17SRHE':'17SRHE',
'AAW':'AAW',
'9CPHS':'9CPHS',
'9INDO':'9INDO',
'9LAL':'9LAL',
'9NFA':'9NFA',
'9WCC':'9WCC',
'9WSP':'9WSP',
'9ZTAM':'9ZTAM',
'9BOR':'9BOR',
'9LAP':'9LAP',
'9ZZANC':'9ZZANC',
'9ZZEXO':'9ZZEXO',
'9ZZLMM':'9ZZLMM',
'9ZZSCCJ':'9ZZSCCJ',
'9ZZSTPSY':'9ZZSTPSY',
'9ZZVARCH':'9ZZVARCH',
'9ZZVGEN':'9ZZVGEN',
'9ZZVPED':'9ZZVPED',
'CIM':'CIM',
'6SAN':'6SAN',
'6JCS':'6JCS',
'6SA':'6SA',
'6AA':'6AA',
'6BIS':'6BIS',
'6BP':'6BP',
'6CAL':'6CAL',
'6GLM':'6GLM',
'6KIC':'6KIC',
'6LAS':'6LAS',
'6LP':'6LP',
'6PMT':'6PMT',
'6RCS':'6RCS',
'6WP':'6WP',
'6ALL':'6ALL',
'6BAW':'6BAW',
'6CA':'6CA',
'6JWC':'6JWC',
'6KIC':'6KIC',
'6SA':'6SA',
'6SAI':'6SAI',
'6SNL':'6SNL',
'AAW':'AAW',
'IRS':'IRS',
'18AHSC':'18AHSC',
'18OBAG':'18OBAG',
'18OCC':'18OCC',
'18OCPA':'18OCPA',
'18OGOS':'18OGOS',
'18OMNH':'18OMNH',
'18OOPD':'18OOPD',
'18OPPS':'18OPPS',
'6IFPA':'6IFPA',
'6PC':'6PC',
'21VMSAR':'21VMSAR',
'NRM':'NRM',
'07CHE':'07CHE',
'07EHCP':'07EHCP',
'07LACS':'07LACS',
'7NL':'7NL',
'14MPT':'14MPT',
'14ND13':'14ND13',
'14ND29':'14ND29',
'14ND38':'14ND38',
'14ND76':'14ND76',
'14NDCC':'14NDCC',
'10AFS':'10AFS',
'10BUSG':'10BUSG',
'10CAL':'10CAL',
'10DE':'10DE',
'10JER':'10JER',
'10KBD':'10KBD',
'10KEN':'10KEN',
'10MAD':'10MAD',
'10PBR':'10PBR',
'10RWS':'10RWS',
'10VLS':'10VLS',
'10WFP':'10WFP',
'10WSC':'10WSC',
'10WT':'10WT',
'10JA':'10JA',
'10LPP':'10LPP',
'10RA':'10RA',
'10RIC':'10RIC',
'10VBS':'10VBS',
'15CWGP':'15CWGP'
}