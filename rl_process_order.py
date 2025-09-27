import pandas as pd 
from rl_helper import (helper_reliance,map_tone,generate_customer_productinstruction
                                ,map_order_group,split_ext_item_id,
                                stamping_instruct,check_style_master,
                                map_and_add_category_column,process_special_remarks,
                                validate_order,adjust_production_delivery_date,fill_missing_style_code,
                                update_special_remarks_with_article_code,convert_excel_to_json
                                )
from reliance_sql_function import fetch_client_data
from etl.reliance.rl_status_update import (get_uploads_collection,update_statuses, update_overall_status,fileurl_update,update_client_name,store_sheet_data,
                                           update_orderpunch_status,sqsresponse_update)
from rl_mapping import mapping_for_quality, RELIANCE_COLUMN_RENAME_MAP
from rl_excelconverter import process_and_export
####from etl.reliance.download_excelfile import process_mainorder_file
import json
import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Setup logger
logger = logging.getLogger(__name__)
import os
import sys

def handle_reliance_client(input_file_path, metadata):
    try:
        document_id = metadata.get('document_id')
        logger.info('Started Processing Relaince Order')
        logger.info(f' This is the document id of metadata {document_id}')
        client_name = metadata.get('client_name')
        order_book_type = metadata.get('order_book_type','Regular Order')
        print(f'This the Order book recieved: {order_book_type}')

        reference_df,  additional_df1 = fetch_client_data(client_name)  
                
        if reference_df.empty:
            logger.error(f"No data found for client {client_name}.")
            raise
        
        reference_df.rename(columns={'GrossWt': 'Gross Wt', 'SKUNo':'Client Style No', 'BaseCollectionName': 'Remark'}, inplace=True)
        logger.info(f' This is the column name of reference_df {reference_df.columns.tolist()}')
        try:
             # Read the Excel file directly from the given path
             rl_df = process_mainorder_file(input_file_path, client_name)
             print(rl_df.head())
        except Exception as e:
             status = 'DOWNLOAD FAILED'
             error_message = f"Failed to read and process files: {e}"
             logger.error(f"{error_message}\n{traceback.format_exc()}")
             return {
                'status': 'error',
                'message': error_message,
                'traceback': traceback.format_exc()
            }
        rl_cleaned = helper_reliance(rl_df)
        logger.info(f"The length of rows in this is {len(rl_cleaned)}")

        rl_cleaned['Metal'] = rl_cleaned['Item Id'].str[0].map(mapping_for_quality)
        rl_cleaned['Tone'] = rl_cleaned['Item Id'].str.get(15).map(map_tone)
        rl_cleaned['CustomerProductionInstruction'] = rl_cleaned['Item Id'].map(generate_customer_productinstruction)
        rl_cleaned['ItemSize'] = rl_cleaned.apply(map_order_group, axis=1)
        rl_cleaned['JOBWORKNUMBER'] = rl_cleaned['Work Order Id']
        
        
        # Mask to identify 'SET' articles with extensions
        # mask = (rl_cleaned['Article code'] == 'SET') & (rl_cleaned['Ext Item Id'].str.contains('\+'))
        mask = (
                        (
                            rl_cleaned['Article code'].str.contains('SET', case=False) | 
                            rl_cleaned['Sub Product Code'].str.contains('SET', case=False)
                        ) &
                        (
                            rl_cleaned['Ext Item Id'].str.contains('\+') | 
                            rl_cleaned['Ext Item Id'].str.contains(r'\b\w+\s*&\s*\w+\b')
                        )
                    )
        if mask.any():
            set_processed = split_ext_item_id(rl_cleaned[mask])
            set_processed['merged_set'] = 1
            main = rl_cleaned[~mask].copy()
            set_processed = stamping_instruct(set_processed)
            merged_checker = check_style_master(set_processed, reference_df)
            merged_checker=map_and_add_category_column(merged_checker, reference_df)
            merged_specialremarks = process_special_remarks(merged_checker)
            

            merged_updated_df = validate_order(merged_specialremarks,reference_df)
            merged_updated_df.rename(columns=RELIANCE_COLUMN_RENAME_MAP, inplace=True)



            merged_final = adjust_production_delivery_date(merged_updated_df)
            logger.info(merged_final.head())
            main['merged_set'] = 0
        else:
            merged_final = None
            main = rl_cleaned.copy()
            main['merged_set'] = 0
        main = stamping_instruct(main)
        if main['Ext Item Id'].isna().any():
                main = fill_missing_style_code(main,additional_df1)
                logger.info('Updated missing value')
        logger.info(main.head(7))
        checker = check_style_master(main, reference_df)
        checker = map_and_add_category_column(checker, reference_df)
        specialremarks = process_special_remarks(checker)
        logger.info('UPDATING SPECIAL REMARKS')
        updated_df = update_special_remarks_with_article_code(specialremarks)
        
        updated_df = validate_order(updated_df,reference_df)
        updated_df.rename(columns=RELIANCE_COLUMN_RENAME_MAP, inplace=True)

        updated_df = adjust_production_delivery_date(updated_df)

        # Save the processed file in the same directory as the input, appending '_processed' to the filename
        base, ext = os.path.splitext(input_file_path)
        processed_file_path = f"{base}_processed{ext}"
        uploadfile_name = process_and_export(updated_df, output_prefix=processed_file_path, set_processed=merged_final)
        logger.info(f"File successfully saved to {uploadfile_name}")
        json_data = convert_excel_to_json(uploadfile_name)
        if json_data:
            for key, value in json_data.items():
                print(f"Processed sheet: {key}")
        return {
            'status': 'success',
            'message': 'File transformation was successful.',
            'output_file': uploadfile_name
        }
    except Exception as e:
         error_reason = str(e)
         logger.error(f'The error occured during reliance transformation : {e}')
         return {
            'status': 'error',
            'message': error_reason
         }

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python rl_process_order.py <input_file_path> <metadata_json>")
        sys.exit(1)
    input_file_path = sys.argv[1]
    metadata_json = sys.argv[2]
    try:
        metadata = json.loads(metadata_json)
    except Exception as e:
        print(f"Error parsing metadata JSON: {e}")
        sys.exit(1)
    result = handle_reliance_client(input_file_path, metadata)
    print(result)
