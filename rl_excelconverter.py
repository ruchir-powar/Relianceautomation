import os
import pandas as pd
from datetime import datetime
from Clients.reliance.rl_mapping import stamping_mapping, reliance_required_columns

# Columns that must NOT appear in the final Excel
def _drop_excluded(df: pd.DataFrame) -> pd.DataFrame:
    # match any variant that trims to JOBWORKNUMBER
    to_drop = [c for c in df.columns if c.strip().upper() == "JOBWORKNUMBER"]
    return df.drop(columns=to_drop, errors="ignore")


def ensure_columns(df, required_columns=reliance_required_columns):
    # Add missing columns with empty values
    for column in required_columns:
        if column not in df.columns:
            if column in ['OrderQty', 'OrderItemPcs']:
                df[column] = 1
            else:
                df[column] = ''
    return df


def filter_columns(df, required_columns=reliance_required_columns):
    # Filter only existing columns from the DataFrame
    available_columns = [col for col in required_columns if col in df.columns]
    out = df[available_columns]
    return _drop_excluded(out)  # ensure banned columns never slip through


def save_to_excel_by_metal(df, output_prefix='output', set_processed=None):
    # Get unique metal qualities
    metal_qualities = df['Metal'].unique()

    file_name = output_prefix if output_prefix.endswith('.xlsx') else f"{output_prefix}.xlsx"

    normal_df = df.copy()

    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:

        # Process "Normal" items
        for metal_quality in metal_qualities:
            metal_normal_df = normal_df[normal_df['Metal'] == metal_quality].copy()
            metal_normal_df['QualityGroup'] = metal_normal_df['OrderGroup'].astype(str).str[-2:]

            for group, group_df in metal_normal_df.groupby('QualityGroup'):
                group_df_filtered = filter_columns(group_df)
                stamping_value = stamping_mapping.get(group, 'UNKNOWN')

                # Build custom sheet name
                today_str = datetime.today().strftime("%d-%m-%y")
                quality = stamping_value
                kt = group_df['KT_final'].iloc[0] if 'KT_final' in group_df.columns else metal_quality
                count = len(group_df_filtered)

                sheet_name = f"{quality} {kt}-{today_str}-{count} PCS"

                # Excel allows max 31 chars for sheet name -> truncate safely
                if len(sheet_name) > 31:
                    sheet_name = sheet_name[:31]

                # Drop excluded columns once more (belt & suspenders)
                group_df_filtered = _drop_excluded(group_df_filtered)
                group_df_filtered.to_excel(writer, sheet_name=sheet_name, index=False)

        # Process set_processed if available
        if set_processed is not None and not set_processed.empty:
            for metal_quality in metal_qualities:
                set_processed_normal_df = set_processed[set_processed['Metal'] == metal_quality].copy()
                set_processed_normal_df['QualityGroup'] = set_processed_normal_df['OrderGroup'].astype(str).str[-2:]

                for group, group_df in set_processed_normal_df.groupby('QualityGroup'):
                    group_df_filtered = filter_columns(group_df)
                    stamping_value = stamping_mapping.get(group, 'UNKNOWN')

                    today_str = datetime.today().strftime("%d-%m-%y")
                    quality = stamping_value
                    kt = group_df['KT_final'].iloc[0] if 'KT_final' in group_df.columns else metal_quality
                    count = len(group_df_filtered)

                    sheet_name = f"{quality} {kt}_merged-{today_str}-{count} PCS"
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]

                    group_df_filtered = _drop_excluded(group_df_filtered)
                    group_df_filtered.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"File saved: {file_name}")
    return file_name


def process_and_export(df, output_prefix='output', set_processed=None):
    # Ensure required columns are present in the DataFrame
    df = ensure_columns(df)
    df = _drop_excluded(df)
    if set_processed is not None and not set_processed.empty:
        set_processed1 = ensure_columns(set_processed)
        set_processed1 = _drop_excluded(set_processed1)
        return save_to_excel_by_metal(df, output_prefix, set_processed1)
    # Save DataFrame into an Excel file and return the file path
    return save_to_excel_by_metal(df, output_prefix, set_processed)
