
import argparse
import sys, types, os, json
import pandas as pd

# --- Module path alias so rl_excelconverter can import 'Clients.reliance.rl_mapping' ---
import rl_mapping as _rl_mapping
Clients = types.ModuleType('Clients')
reliance = types.ModuleType('Clients.reliance')
reliance.rl_mapping = _rl_mapping
Clients.reliance = reliance
sys.modules['Clients'] = Clients
sys.modules['Clients.reliance'] = reliance
sys.modules['Clients.reliance.rl_mapping'] = _rl_mapping

# --- Now import uploaded modules ---
import rl_helper as H
import rl_mapping as M
import rl_excelconverter as XL

def read_mainorder_file(path: str) -> pd.DataFrame:
    """Read the Reliance main order Excel (first sheet)."""
    return pd.read_excel(path, sheet_name=0, dtype=str)

def load_style_master_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Harmonize a few column names that the pipeline expects
    # Expecting columns like: 'StyleCode','PartyName','MainGroupPrdctCtg','SubGroupPrdctCtg','Client Style No'
    # If user has SKUNo instead of 'Client Style No', mirror it.
    if 'Client Style No' not in df.columns and 'SKUNo' in df.columns:
        df = df.rename(columns={'SKUNo': 'Client Style No'})
    return df

def load_validator_csv(path: str) -> pd.DataFrame:
    # Expecting columns: 'RRLDsgCd','AuraDsgCd'
    return pd.read_csv(path)

def ensure_column(df, name, default=''):
    if name not in df.columns:
        df[name] = default
    return df

def run_offline(input_xlsx: str, client_name: str, output_prefix: str = None,
                style_master_csv: str = None, validator_csv: str = None):
    # 1) Load input order book
    rl_df = read_mainorder_file(input_xlsx)

    # 2) Clean / aggregate diamonds, dedupe WO Srl
    rl_cleaned = H.helper_reliance(rl_df)

    # 3) Basic derived columns
    rl_cleaned['Metal'] = rl_cleaned['Item Id'].str[0].map(M.mapping_for_quality)
    rl_cleaned['Tone'] = rl_cleaned['Item Id'].str.get(15).map(H.map_tone)
    rl_cleaned['CustomerProductionInstruction'] = rl_cleaned['Item Id'].map(H.generate_customer_productinstruction)
    rl_cleaned['ItemSize'] = rl_cleaned.apply(H.map_order_group, axis=1)
    rl_cleaned['JOBWORKNUMBER'] = rl_cleaned.get('Work Order Id', rl_cleaned.get('Work Order Id ', ''))

    # 4) Split SETs if Ext Item Id contains '+' or '<a & b>' pattern
    mask = (
        (rl_cleaned['Article code'].astype(str).str.contains('SET', case=False) |
         rl_cleaned['Sub Product Code'].astype(str).str.contains('SET', case=False)) &
        (rl_cleaned['Ext Item Id'].astype(str).str.contains('\+') |
         rl_cleaned['Ext Item Id'].astype(str).str.contains(r'\b\w+\s*&\s*\w+\b'))
    )

    style_master = None
    validator = None
    if style_master_csv and os.path.exists(style_master_csv):
        style_master = load_style_master_csv(style_master_csv)
    if validator_csv and os.path.exists(validator_csv):
        validator = load_validator_csv(validator_csv)

    def safe_check_style_master(df):
        if style_master is None:
            df = ensure_column(df, 'Checking_set', 0)
            return df
        return H.check_style_master(df, style_master)

    def safe_map_and_add_category(df):
        if style_master is None:
            df = ensure_column(df, 'withchain', '')
            return df
        return H.map_and_add_category_column(df, style_master)

    def safe_fill_missing_style_code(df):
        if validator is None:
            return df
        return H.fill_missing_style_code(df, validator)

    def safe_validate(df):
        if style_master is None:
            return df
        # style_master must expose 'StyleCode','DiamondWt','DiamondPcs'
        needed = {'StyleCode','DiamondWt','DiamondPcs'}
        if not needed.issubset(set(style_master.columns)):
            return df
        return H.validate_order(df, style_master.rename(columns=lambda c: c))

    if mask.any():
        set_processed = rl_cleaned[mask].copy()
        set_processed['merged_set'] = 1
        main = rl_cleaned[~mask].copy()
        set_processed = H.stamping_instruct(set_processed)
        set_processed = safe_check_style_master(set_processed)
        set_processed = safe_map_and_add_category(set_processed)
        set_processed = H.process_special_remarks(set_processed)
        set_processed = safe_validate(set_processed)
        set_processed.rename(columns=M.RELIANCE_COLUMN_RENAME_MAP, inplace=True)
        merged_final = H.adjust_production_delivery_date(set_processed)
        main['merged_set'] = 0
    else:
        merged_final = None
        main = rl_cleaned.copy()
        main['merged_set'] = 0

    # Main branch
    main = H.stamping_instruct(main)
    main = safe_fill_missing_style_code(main)
    main = safe_check_style_master(main)
    main = safe_map_and_add_category(main)
    main = H.process_special_remarks(main)
    main = H.update_special_remarks_with_article_code(main)
    main = safe_validate(main)
    main.rename(columns=M.RELIANCE_COLUMN_RENAME_MAP, inplace=True)
    main = H.adjust_production_delivery_date(main)

    # Output
    base, ext = os.path.splitext(input_xlsx)
    output_prefix = output_prefix or f"{base}_processed"
    output_file = XL.process_and_export(main, output_prefix=output_prefix, set_processed=merged_final)
    return output_file

def main():
    p = argparse.ArgumentParser(description="Offline runner for Reliance order processing (no DB).")
    p.add_argument('--input', required=True, help='Path to Reliance order Excel (.xlsx)')
    p.add_argument('--client', default='Reliance', help='Client name (default: Reliance)')
    p.add_argument('--output-prefix', default=None, help='Output file prefix (without .xlsx)')
    p.add_argument('--style-master', default=None, help='Optional CSV with style master (PartyStyleMst projection)')
    p.add_argument('--validator', default=None, help='Optional CSV with RRLDsgCd→AuraDsgCd mapping')
    args = p.parse_args()

    out = run_offline(args.input, args.client, args.output_prefix, args.style_master, args.validator)
    print(json.dumps({"status": "success", "output_file": out}))

if __name__ == '__main__':
    main()
