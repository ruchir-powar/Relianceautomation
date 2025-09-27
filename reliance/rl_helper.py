import pandas as pd
from io import BytesIO
from rl_mapping import tone_mapping,stamping_mapping
from rl_mapping import rng_mapping, brc_mapping, bng_mapping, msr_mapping, last_two_digit_mapping, first_digit_mapping
from rl_mapping import valid_product_categories, order_group_mapping, article_code_mapping
import logging
from datetime import timedelta
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import json
import traceback


def helper_reliance(actual_order, threshold=0.99):
    try:
        logging.info(f"Initial DataFrame shape: {actual_order.shape}")
        
        # Remove the last 4 rows
        actual_order = actual_order.iloc[:-4]
        logger.info(f"After removing the last 4 rows: {actual_order.shape}")
        
        # Drop columns with more than 99% NaN values
        actual_order = actual_order.dropna(thresh=len(actual_order) * (1 - threshold), axis=1)
        logger.info(f"After dropping columns with > {threshold*100}% NaN values: {actual_order.shape}")
        
        # Filter rows where 'Item Id Stone' is 'DRD-IGI' and group by 'WO Srl' to get the sum of 'Qty.1'
        filtered_sum = actual_order[actual_order['Item Id Stone'] == 'DRD-IGI']\
                       .groupby('WO Srl')['Qty.1'].sum().round(4)
        logger.info("Filtered and grouped by 'WO Srl' to get diamond weight sum.")

        filtered_sum_diamond_pieces = actual_order[actual_order['Item Id Stone'] == 'DRD-IGI']\
                              .groupby('WO Srl')['Pds CW Qty'].sum().round(4)
        
        # Map diamond pieces sum to 'Diamond Pieces' column
        
        # Remove duplicates, keeping only the first instance of 'WO Srl'
        unique_srl_df = actual_order.drop_duplicates(subset='WO Srl', keep='first').copy(deep=True)
        logger.info(f"After removing duplicate 'WO Srl' entries: {unique_srl_df.shape}")
        
        unique_srl_df['Dia Wt'] = unique_srl_df['WO Srl'].map(filtered_sum)
        unique_srl_df['Diamond Pieces'] = unique_srl_df['WO Srl'].map(filtered_sum_diamond_pieces)
        unique_srl_df['Diamond Pieces'] = unique_srl_df['Diamond Pieces'].fillna(0)
        # Fill NaN values with 0 for rows where 'Item Id Stone' is not 'DRD-IGI'
        unique_srl_df['Dia Wt'] = unique_srl_df['Dia Wt'].fillna(0)
        logger.info("Mapped diamond weights and filled NaN values.")
        logger.info("Mapped diamond weights and diamond pieces, and filled NaN values.")
        # Log the final shape of the DataFrame
        logger.info(f"Final DataFrame shape: {unique_srl_df.shape}")
        # Drop rows where 'column_name' is NaN or missing
        unique_srl_df = unique_srl_df.dropna(subset=['Item Id'])

        return unique_srl_df

    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")
        return actual_order


def map_tone(digit):
    
    try:
        # Map the digit using the tone_mapping dictionary
        return tone_mapping.get(digit, digit)  # Use the digit itself if not found in the mapping
    except Exception as e:
        logging.error(f'Unexpected error: {e}')
        return None
    
def generate_customer_productinstruction(item_code): 
    # Extract the last two digits
    
    last_two_digits = item_code[-2:] if len(item_code) >= 2 else ''
    # Map the last two digits to the stamping value
    stamping_value = stamping_mapping.get(last_two_digits, 'UNKNOWN')
    # Return the formatted string
    return f"IGI CERT-({stamping_value}), HALLMARK (STAMPING ON BOTH PART)"

def map_order_group(row):
    try:
        article_code = row['Article code']
        order_group = row['Item Id'][11:13] if len(row['Item Id']) >= 13 else ''

        if article_code == 'RNG':
            return rng_mapping.get(order_group, '')
        elif article_code in ['BRC','BAN','BLT']:
            return brc_mapping.get(order_group, '')
        elif article_code in ['BNG','BAG']:
            return bng_mapping.get(order_group, '')
        elif article_code in ['MSR']:
            return msr_mapping.get(order_group, '')
        elif article_code == 'ERG':
          if len(row['Item Id']) >= 15 and row['Item Id'][13:15] in ['71', '76']:
            return ''
          elif len(row['Item Id']) >= 15:
            return row['Item Id'][13:15]
          else:
            return ''
        elif article_code == 'PDC':
          
          return msr_mapping.get(order_group, '') + ' INCH'
        else: 
            return ''
    except Exception as e:
        logger.error(f'Error processing row: {row} - {e}')
        return ''
    
def split_ext_item_id(df):

    # Splitting rows with '+'
    rows_to_split = df.copy()
    rows_to_split['Ext Item Id'] = rows_to_split['Ext Item Id'].apply(
        lambda x: x[:-2] if x.endswith('DT') else x
    )
    rows_to_split['Ext Item Id'] = rows_to_split['Ext Item Id'].str.split('\+')

    # Expanding the split rows into multiple rows
    df_processed = rows_to_split.explode('Ext Item Id')

    return df_processed


def stamping_instruct(df):
    try:
        # Ensure the DataFrame has the necessary columns
        if 'Item Id' not in df.columns or 'Article code' not in df.columns:
            raise ValueError("DataFrame must contain 'Item Id' and 'Article Code' columns")

        # Extract the first character and the last two characters
        first_chars = df['Item Id'].str[0]
        last_two_chars = df['Item Id'].str[-2:]

        # Map the extracted characters to the desired values
        first_char_mapped = first_chars.map(first_digit_mapping)
        last_two_chars_mapped = last_two_chars.map(last_two_digit_mapping)

        # Initialize the 'Stamping' column
        df['StampInstruction'] = (
            first_char_mapped.fillna('') + ', ' +
            last_two_chars_mapped.fillna('') + '-DIA.WT,CS.WT'
        )

        # Update 'Stamping' based on 'Article Code'
        # df.loc[df['Article code'] == 'SET', 'StampInstruction'] = (
        #     first_char_mapped.fillna('') + ', ' +
        #     last_two_chars_mapped.fillna('') + '-SET DIA.WT,CS.WT'
        # )
        df.loc[(df['Article code'] == 'SET') & (df['merged_set'] == 1), 'StampInstruction'] = (
    first_char_mapped.fillna('') + ', ' +
    last_two_chars_mapped.fillna('') + '-SET DIA.WT, SET CS.WT'
)

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    return df

def check_style_master(df, client_style_master):
    # Initialize the 'Checking_set' column to 0 by default
    df['Checking_set'] = 0

    # Filter rows where Article code is not 'SET'
    filtered_rows = df[df['Article code'] != 'SET']

    # Loop through each filtered row to check for matches
    for index, row in filtered_rows.iterrows():
        # Get the last two digits from the 'Item Id'
        last_two_digits = row['Item Id'][-2:]

        # Get the corresponding party name from the mapping
        party_name = last_two_digit_mapping.get(last_two_digits)

        if party_name:
            # Create the search string for matching
            search_string = f"Reliance Retail Ltd {party_name}"

            # Check if the Ext Item Id exists in client_style_master for the given party name
            matches = client_style_master[
                (client_style_master['Client Style No'] == row['Ext Item Id']) &
                (client_style_master['PartyName'] == search_string)&
                (client_style_master['SubGroupPrdctCtg'].isin(valid_product_categories))
            ]

            # If matches exist, update the Checking_set column
            if not matches.empty:
                df.at[index, 'Checking_set'] = 1

    return df

def map_and_add_category_column(df1, df2):

    """
    Maps a category column from df2 to df1 based on matching key columns and adds it to df1.

    Assumes:
    - `df1` contains a key column named 'Ext Item Id'.
    - `df2` contains a key column named 'StyleCode' and a value column named 'category'.
    - The new column added to `df1` will be named 'withchain'.

    :param df1: DataFrame to which the column is added
    :param df2: DataFrame providing the mapping information
    :return: Updated DataFrame with the new column or original DataFrame in case of error
    """
    try:
        # Make a copy of df1
        df1_copy = df1.copy()

        # Create a mapping dictionary from df2
        mapping_dict = df2.set_index('StyleCode')['MainGroupPrdctCtg'].to_dict()

        # Use .map() to fill the 'withchain' column in df1
        df1_copy['withchain'] = df1_copy['Ext Item Id'].map(mapping_dict)

        logger.info("Column 'withchain' successfully added to the DataFrame.")
        return df1_copy

    except Exception as e:
        logger.error(f"An error occurred while mapping and adding the 'withchain' column: {e}")
        # Return the original DataFrame in case of error
        return df1
    
def process_special_remarks(df):
    try:

        # Ensure the DataFrame has the necessary columns
        required_columns = ['Article code', 'Dia Wt', 'Item Id']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain {', '.join(required_columns)} columns")

        # Step 1: Create the base 'Special Remarks' based on 'Article Code'
        # df['SpecialRemarks'] = df.apply(
        #     lambda row: f"MAINTAIN SET DIA.WT- {row['Dia Wt']} CTS, DIA TOL (+ - 3%)"
        #     if row['Article code'] == 'SET'  and row['merged_set'] == 1 else f"MAINTAIN DIA.WT- {row['Dia Wt']} CTS, DIA TOL (+ - 3%)",
        #     axis=1
        # )
        df['SpecialRemarks'] = df.apply(
            lambda row: f"MAINTAIN SET DIA.WT- {row['Dia Wt']} CTS, DIA TOL (+ - 3%)"
            if ('SET' in str(row['Article code']).upper() or 'SET' in str(row['Sub Product Code']).upper())  and row['merged_set'] == 1 else f"MAINTAIN DIA.WT- {row['Dia Wt']} CTS, DIA TOL (+ - 3%)",
            axis=1
        )

        # Step 2: Handle the prefix based on 'OrderGroup' 14-15 digits for 'ERG'
        erg_condition = df['Article code'].isin(['ERG', 'NSO', 'NSP'])
        if erg_condition.any():
            df['OrderGroup_14_15'] = df.loc[erg_condition, 'Item Id'].str[13:15]

            # Map the OrderGroup 14-15 values to the prefix from order_group_mapping
            df.loc[erg_condition, 'SpecialRemarks'] = (
                df.loc[erg_condition, 'OrderGroup_14_15'].map(order_group_mapping).fillna('') + " " + df['SpecialRemarks']
            )

            # Drop the temporary column after processing
            df.drop(columns=['OrderGroup_14_15'], inplace=True)

        # Step 3: Add suffixes for specific Article Codes
        df.loc[df['Article code'].isin(['MSR']), 'SpecialRemarks'] += " WITH CHAIN"
        #df.loc[df['Article code'] == 'BLT', 'SpecialRemarks'] += " MAKE ONLY BRACLET"
        condition = df['Article code'].isin(['PDC']) | df['Sub Product Code'].isin(['PDC','NECKLACE SET'])

        # Append " WITH CHAIN" to 'SpecialRemarks' for rows that meet the condition
        df.loc[condition, 'SpecialRemarks'] += " WITH CHAIN"
        condition2 = (
        df['withchain'].str.contains('NECKLACE', na=False, case=False) &
        ~df['SpecialRemarks'].str.contains('WITH CHAIN', na=False, case=False)
    )

    # Append " WITH CHAIN" to 'SpecialRemarks' for rows that meet the condition
        df.loc[condition2, 'SpecialRemarks'] += " WITH CHAIN"
        condition_bangle = df['withchain'].str.contains('BANGLE', na=False, case=False)

        for idx, row in df.loc[condition_bangle].iterrows():
            try:
                item_id_code = row['Item Id'][13:15]
                
                if item_id_code == 'B1':
                    df.loc[idx, 'SpecialRemarks'] = (
                        (df.loc[idx, 'SpecialRemarks'] or '') + 
                        " QUANTITY FOR SINGLE BANGLE (MAKE SINGLE PCS)"
                    )
                elif item_id_code == 'B2':
                    df.loc[idx, 'SpecialRemarks'] = (
                        (df.loc[idx, 'SpecialRemarks'] or '') + 
                        " [QUANTITY FOR PAIR BANGLE, MAKE PAIR]"
                    )
                    df.loc[idx, 'CustomerProductionInstruction'] = (
                        (df.loc[idx, 'CustomerProductionInstruction'] or '') + 
                        " NEED CERTIFICATE OF PAIR"
                    )
                
            except Exception as e:
                logger.error(f"Error processing row {idx}: {e}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    return df

def validate_order(actual_order_df, style_code_df):

    # Check if the required columns exist in the DataFrames
    required_columns_actual = ['Ext Item Id', 'Dia Wt', 'Diamond Pieces']
    required_columns_reference = ['StyleCode', 'DiamondWt', 'DiamondPcs']
    
    missing_actual_columns = [col for col in required_columns_actual if col not in actual_order_df.columns]
    missing_reference_columns = [col for col in required_columns_reference if col not in style_code_df.columns]
    
    if missing_actual_columns or missing_reference_columns:
        # Display meaningful error messages
        error_message = "Missing required columns:\n"
        if missing_actual_columns:
            error_message += f"- In actual_order_df: {', '.join(missing_actual_columns)}\n"
        if missing_reference_columns:
            error_message += f"- In style_code_df: {', '.join(missing_reference_columns)}"
        raise ValueError(error_message)
    
    # Create a copy of the actual_order DataFrame to avoid modifying the original
    actual_order_copy = actual_order_df.copy(deep=True)
    
    # Initialize the Error and Wrong Style Code columns
    actual_order_copy['Error'] = None
    actual_order_copy['wrong_style_code'] = 0

    # Iterate over rows in actual_order_copy
    for index, row in actual_order_copy.iterrows():
        try:
            item_id = row['Ext Item Id']
            
            # Check if Item Id is in the reference style_code_df
            if item_id in style_code_df['StyleCode'].values:
                # Fetch the corresponding reference row for the current item_id
                reference_row = style_code_df[style_code_df['StyleCode'] == item_id].iloc[0]
                 
                # Validate Dia Wt and Diamond Pieces
                errors = []
                tolerance = 0.03 * reference_row['DiamondWt']
                lower_bound = reference_row['DiamondWt'] - tolerance
                upper_bound = reference_row['DiamondWt'] + tolerance
                # print(f"This is the Order diamond pieces {row['Diamond Pieces']} and this is the Master Diamond pieces {reference_row['DiamondPcs']}")
                
                if not (lower_bound <= row['Dia Wt'] <= upper_bound):
                    errors.append(f"Dia Wt doesn't match (±3% tolerance), This is the master diamond weight {reference_row['DiamondWt']}")
                    # print(f"Dia Wt doesn't match (±3% tolerance), This is the master diamond weight {reference_row['DiamondWt']}")
                if row['Diamond Pieces'] != reference_row['DiamondPcs']:
                    errors.append(f"Diamond Pieces didn't match, This is the master diamond pieces {reference_row['DiamondPcs']}")
                if row['withchain'] == 'BANGLE':
                    errors.append('PLEASE CHECK BANGLE SIZE')
                    # print(f"Diamond Pieces didn't match, This is the master diamond pieces {reference_row['DiamondPcs']}")
                # Update Error column if there are any validation errors
                if errors:
                    actual_order_copy.at[index, 'Error'] = ', '.join(errors)
            
            else:
                # If Item Id is not found in reference, mark Wrong Style Code as 1
                actual_order_copy.at[index, 'wrong_style_code'] = 1
                actual_order_copy.at[index, 'Error'] = "Design number is wrong"
        
        except Exception as e:
            # Log the exception and set an error message in the Error column
            actual_order_copy.at[index, 'Error'] = f"Error processing row: {str(e)}"
            logger.error(f"Exception occurred for row {index}: {e}")
    return actual_order_copy 

def adjust_production_delivery_date(df):
    # Ensure 'Expecteddeliverydate' is in datetime format
    df['Expecteddeliverydate'] = pd.to_datetime(df['Expecteddeliverydate'], errors='coerce', dayfirst=True)

    # Calculate 'Productiondeliverydate' as 5 days before 'Expecteddeliverydate'
    df['Productiondeliverydate'] = df['Expecteddeliverydate'] - timedelta(days=5)

    # Format both dates as 'dd-mm-yyyy'
    df['Expecteddeliverydate'] = df['Expecteddeliverydate'].dt.strftime('%d-%m-%Y')
    df['Productiondeliverydate'] = df['Productiondeliverydate'].dt.strftime('%d-%m-%Y')

    return df

def fill_missing_style_code(df, reference_df):
    try:
        logger.info('started missing values')
        # Make a copy of the original DataFrame to avoid modifying it directly
        df_copy = df.copy()
        
        # Check for missing values in StyleCode and proceed if any are found
        if df_copy['Ext Item Id'].isna().any():
            # Filter rows where StyleCode is missing
            missing_style_rows = df_copy['Ext Item Id'].isna()

            # Remove duplicates from reference_df based on 'RRLDsgCd' and keep the first occurrence
            reference_unique = reference_df.drop_duplicates(subset='RRLDsgCd')

            # Map ItemRefNo to AuraDsgCd based on matching RRLsgCd in reference_df
            df_copy.loc[missing_style_rows, 'Ext Item Id'] = df_copy.loc[missing_style_rows, 'Item Id'].map(
                reference_unique.set_index('RRLDsgCd')['AuraDsgCd']
            )
        
        return df_copy  # Return the modified copy
    
    except KeyError as e:
        logger.error(f"Column missing: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return df  # Return the original DataFrame if an error occurs

def update_special_remarks_with_article_code(df):
    
    # Check if the required columns exist in the DataFrame
    if 'Checking_set' not in df.columns or 'SpecialRemarks' not in df.columns or 'Article code' not in df.columns:
        raise ValueError("DataFrame must contain 'Checking_set', 'SpecialRemarks', and 'Article code' columns.")

    # Apply the mapping where 'Checking_set' is 1 and append 'MAKE ONLY {article description}' to 'SpecialRemarks'
    df.loc[df['Checking_set'] == 1, 'SpecialRemarks'] += df['Article code'].map(
        lambda x: f" MAKE ONLY {article_code_mapping[x]}" if x in article_code_mapping else ''
    ).fillna('')
    return df
from io import BytesIO
def convert_excel_to_json(file_name):
    """
    Convert an Excel file with multiple sheets into a JSON object stored in a variable.
    
    Args:
        file_name (str): The path to the Excel file.
    
    Returns:
        dict: A dictionary where keys are sheet names and values are JSON objects.
    """
    try:
        with open(file_name, 'rb') as file:
            # Load the Excel file
             file_content = BytesIO(file.read()) 

        excel_data = pd.ExcelFile(file_content)
        logger.info(excel_data)
        # Load the Excel file
        # excel_data = pd.ExcelFile(file_name)

        # Initialize a dictionary to store JSON data
        excel_json = {}

        # Convert each sheet into JSON
        for sheet_name in excel_data.sheet_names:
            sheet_df = excel_data.parse(sheet_name)
            excel_json[sheet_name] = json.loads(sheet_df.to_json(orient="records"))

        logger.info("Excel data successfully converted to JSON.")
        return excel_json

    except Exception as e:
        error_message = f"Error converting Excel to JSON: {e}\n{traceback.format_exc()}"
        logger.error(error_message)
        return None
