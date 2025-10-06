import os
import pandas as pd
from datetime import datetime
from Clients.reliance.rl_mapping import stamping_mapping, reliance_required_columns

# --- Global default note to appear in every row ---
GLOBAL_SPECIAL_REMARK = "MAINTAIN DIA.WT- 0.03 CTS,DIA TOL (+ - 3%),"

# Columns that must NOT appear in the final Excel
def _drop_excluded(df: pd.DataFrame) -> pd.DataFrame:
    # match any variant that trims to JOBWORKNUMBER
    to_drop = [c for c in df.columns if str(c).strip().upper() == "JOBWORKNUMBER"]
    return df.drop(columns=to_drop, errors="ignore")

def _force_default_sr(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure SpecialRemarks exists and is set ONLY to the standard note.
    This blocks any article/code prefixes or other upstream text.
    """
    # Remove alternate header if present so we keep one canonical column
    if "Special Remarks" in df.columns and "SpecialRemarks" not in df.columns:
        df = df.rename(columns={"Special Remarks": "SpecialRemarks"})
    if "SpecialRemarks" not in df.columns:
        df["SpecialRemarks"] = GLOBAL_SPECIAL_REMARK
    else:
        df["SpecialRemarks"] = GLOBAL_SPECIAL_REMARK
    return df

def ensure_columns(df, required_columns=reliance_required_columns):
    # Add missing columns with empty values (or defaults)
    for column in required_columns:
        if column not in df.columns:
            if column in ['OrderQty', 'OrderItemPcs']:
                df[column] = 1
            else:
                df[column] = ''
    # Make sure SpecialRemarks exists even if not in required_columns
    if "SpecialRemarks" not in df.columns:
        df["SpecialRemarks"] = ""
    return df

def filter_columns(df, required_columns=reliance_required_columns):
    """
    Keep columns in required_columns (that exist), but also ensure SpecialRemarks
    is present in the output (even if not listed in required_columns).
    """
    available_columns = [col for col in required_columns if col in df.columns]
    out = df.loc[:, available_columns].copy()

    # Ensure SpecialRemarks is retained
    if "SpecialRemarks" in df.columns and "SpecialRemarks" not in out.columns:
        out["SpecialRemarks"] = df["SpecialRemarks"].values

    # Drop banned cols like JOBWORKNUMBER
    out = _drop_excluded(out)
    return out

def _sheet_name(quality: str, kt: str, count: int, merged: bool = False) -> str:
    today_str = datetime.today().strftime("%d-%m-%y")
    suffix = f"_merged-{today_str}-{count} PCS" if merged else f"-{today_str}-{count} PCS"
    name = f"{quality} {kt}{suffix}"
    # Excel sheet name max length = 31
    return name[:31] if len(name) > 31 else name

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

                # Determine KT & count
                kt = (group_df['KT_final'].iloc[0] if 'KT_final' in group_df.columns else metal_quality) or ''
                kt = str(kt)
                count = len(group_df_filtered)

                # Enforce SpecialRemarks and drop excluded columns
                group_df_filtered = _force_default_sr(group_df_filtered)
                group_df_filtered = _drop_excluded(group_df_filtered)

                # Build sheet name and write
                sheet_name = _sheet_name(stamping_value, kt, count, merged=False)
                group_df_filtered.to_excel(writer, sheet_name=sheet_name, index=False)

        # Process set_processed if available
        if set_processed is not None and not set_processed.empty:
            for metal_quality in metal_qualities:
                set_processed_normal_df = set_processed[set_processed['Metal'] == metal_quality].copy()
                set_processed_normal_df['QualityGroup'] = set_processed_normal_df['OrderGroup'].astype(str).str[-2:]

                for group, group_df in set_processed_normal_df.groupby('QualityGroup'):
                    group_df_filtered = filter_columns(group_df)
                    stamping_value = stamping_mapping.get(group, 'UNKNOWN')

                    kt = (group_df['KT_final'].iloc[0] if 'KT_final' in group_df.columns else metal_quality) or ''
                    kt = str(kt)
                    count = len(group_df_filtered)

                    group_df_filtered = _force_default_sr(group_df_filtered)
                    group_df_filtered = _drop_excluded(group_df_filtered)

                    sheet_name = _sheet_name(stamping_value, kt, count, merged=True)
                    group_df_filtered.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"File saved: {file_name}")
    return file_name

def process_and_export(df, output_prefix='output', set_processed=None):
    # Ensure required columns are present in the DataFrame
    df = ensure_columns(df)
    # Always enforce default SpecialRemarks + drop excluded before writing
    df = _force_default_sr(df)
    df = _drop_excluded(df)

    if set_processed is not None and not set_processed.empty:
        set_processed1 = ensure_columns(set_processed)
        set_processed1 = _force_default_sr(set_processed1)
        set_processed1 = _drop_excluded(set_processed1)
        return save_to_excel_by_metal(df, output_prefix, set_processed1)

    # Save DataFrame into an Excel file and return the file path
    return save_to_excel_by_metal(df, output_prefix, set_processed)
