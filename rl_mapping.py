mapping_for_quality = {
    '2': 'GA22',
    '3': 'GA23',
    '6': 'GA24(99.5)',
    '4': 'GA24(99.9)',
    '7': 'GA14',
    '8': 'GA18',
    '9': 'GA09',
    'S': 'S999',
    'A': 'S925',
    'P': 'PT95',
    'W': 'GAWHI18KT',
    'Y': 'GAWHI14KT',
    'Z': 'GAWHI9KT'
}

tone_mapping = {
    'R': 'P',
    'U': 'PY',
    'V': 'YW',
    'X': 'PW',
    'Z': 'PYW'
}

stamping_mapping = {
    'BC': 'VVS-GH',
    'BB': 'VVS-EF',
    'EC': 'VS-GH',
    'HD': 'SI-JK',
    'HC': 'SI-GH',
    'ED': 'VS-IJ',
    'BD': 'VVS-IJ'
}

# Mappings for order group (from map_order_group in rl_helper.py)
rng_mapping = {
    'A1': '04', 'A2': '06', 'A3': '07', 'A4': '09', 'A5': '10',
    'A6': '12', 'A7': '13', 'A8': '15', 'A9': '17', 'B1': '18',
    'B2': '20', 'B3': '21', 'B4': '23', 'B5': '24', 'B6': '26',
    'B7': '28', 'B8': '29', 'B9': '31', 'C1': '32'
}

brc_mapping = {
    'E2': '2.2 OPN', 'E3': '2.3 OPN', 'E4': '2.4 OPN', 'E5': '2.6 OPN',
    'A5': '6 INCH', 'A6': '6.5 INCH ', 'A7': '7 INCH', 'A8': '7.5 INCH', 'A9': '8 INCH',
    'B1': '8.5 INCH', 'B2': '9 INCH', 'B3': '9.5', 'B6': '2.4 OPN',
    'B8': '2.6 AANI', 'A8': 'FS', 'B9': '2.7 AANI', 'C1': '2.8 AANI',
}

bng_mapping = {
    'B6': '2.4 OPN', 'B8': '2.6 AANI', 'A8': 'FS', 'B9': '2.7 AANI', 'C1': '2.8 AANI'
}

msr_mapping = {
    'A8': '16', 'B1': '18', 'B3': '20', 'B5': '22'
}

# Mappings for stamping_instruct (from rl_helper.py)
last_two_digit_mapping = {
    'BC': 'D', 'BB': 'C', 'EC': 'E', 'HD': 'F',
    'HC': 'G', 'ED': 'X', 'BD': 'Z'
}

first_digit_mapping = {
    '2': '22KT', '3': '23KT', '6': '24KT', '4': '24KT',
    '7': '14KT', '8': '18KT', '9': '9KT', 'S': 'S999',
    'A': 'S925', 'P': 'PT95', 'W': 'GAWHI18KT', 'Y': 'GAWHI14KT',
    'Z': 'GAWHI9KT'
}

# Valid product categories for style master checking (from check_style_master in rl_helper.py)
valid_product_categories = [
    'ENS BRACELETE', 'ENS PENDANT', 'ENS NECKLACE', 'ENS EARRING', 'ENS RING',
    'NLS EARRING', 'NLS NECKLACE', 'PDS PENDANT', 'ENS TANMANYA', 'PDS EARRING',
    'SET ENS NECKLACE', 'SET ENS NECKLACE EARRING', 'SET ENS LADIES RING',
    'ENS TANMANIYA EARRING', 'SET ENS BANGLE', 'SET ENS PENDANT EARRING',
    'SET ENS PENDANT', 'SET ENS TANMANYA EARRING', 'SET ENS BRACELET',
    'SET ENS TANMANYA', 'SET ENS NOSEPIN', 'SET ENS OVEL BANGLE (BRC)',
    'SET ENS NECKLET', 'SET ENS MANG TIKKA', 'NECKLET SET', 'NECKLET EARRING'
]

# Order group mapping for special remarks (from process_special_remarks in rl_helper.py)
order_group_mapping = {
    'A1': 'Prefix_A1',
    'A2': 'Prefix_A2',
    'B1': 'QUANTITY FOR SINGLE BANGLE (MAKE SINGLE PCS)',
    'B2': '[QUANTITY FOR PAIR BANGLE, MAKE PAIR]',
    '75': '75 (FULL BALI)',
    '72': '72 (USE SOUTH SCREW)',
    '70': '70 (USE TAR)'
}

# Article code mapping for special remarks (from update_special_remarks_with_article_code in rl_helper.py)
article_code_mapping = {
    'ERG': 'EARRING',
    'BLT': 'BRACELET',
    'BNG': 'BANGLE',
    'NLS': 'NECKLACE',
    'NKL': 'NECKLACE',
    'RNG': 'RING',
    'PDC': 'PENDANT',
    'PDT': 'PENDANT',
}

# Required columns for Titan Excel export (from titan/excelconverter.py)
titan_required_columns = [
    'Customer', 'OrderType', 'MakeType', 'OrderDate', 'DelDate', 'MfgDate', 'DiaReqDate', 'OrderBy',
    'OrderRemarks', 'BatchNo', 'ProdCode', 'LineId', 'CatelogNo/Style No', 'MetalShape', 'Purity',
    'METALCOLOR', 'Qty', 'ItemSize', 'Stamping', 'CertName', 'Remarks', 'DMetalGroup', 'DMetalShape',
    'PLATING', 'MinWt', 'MaxWt', 'DesignProductionInstruction', 'CustomerProductionInstruction',
    'OrderNumber', 'ReqNo', 'VendorSite', 'TitanCollection', 'TitanJobworkNo', 'TitanDVRNO',
    'TitanOfflineNo', 'TitanTOCOrderType'
]

# Required columns for Reliance Excel export (from reliance/rl_excelconverter.py)
reliance_required_columns = [
    'SrNo', 'StyleCode', 'ItemSize', 'OrderQty', 'OrderItemPcs', 'Metal', 'Tone', 'ItemPoNo',
    'ItemRefNo', 'StockType', 'MakeType', 'CustomerProductionInstruction', 'SpecialRemarks',
    'DesignProductionInstruction', 'StampInstruction', 'OrderGroup', 'Certificate', 'SKUNo',
    'Basestoneminwt', 'Basestonemaxwt', 'Basemetalminwt', 'Basemetalmaxwt',
    'Productiondeliverydate', 'Expecteddeliverydate', 'SetPrice', 'StoneQuality',
    'Article code', 'Sub Product Code','Error'
]

# Centralized column rename mapping for Reliance processing
RELIANCE_COLUMN_RENAME_MAP = {
    'Item Id': 'OrderGroup',
    'Indent Name': 'ItemPoNo',
    'Special Remarks': 'ItemRefNo',
    'Ext Item Id': 'StyleCode',
    'Min Wt': 'Basemetalminwt',
    'Max Wt': 'Basemetalmaxwt',
    'Target Date': 'Expecteddeliverydate',
    'SKU Number': 'SKUNo'
}
