import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import io
import re
from typing import List, Dict, Tuple, Optional
import PyPDF2
import openpyxl
from docx import Document
import json

# Set page config
st.set_page_config(page_title="Reconciliation Tool", layout="wide", page_icon="📊")

class DataExtractor:
    """Extract data from various file formats"""
    
    @staticmethod
    def extract_from_excel(file) -> Dict[str, pd.DataFrame]:
        """Extract all sheets from Excel file"""
        try:
            excel_file = pd.ExcelFile(file)
            sheets = {}
            for sheet_name in excel_file.sheet_names:
                df_preview = pd.read_excel(file, sheet_name=sheet_name, header=None, nrows=10)
                
                header_row = 0
                max_non_empty = 0
                
                for idx in range(min(5, len(df_preview))):
                    non_empty = df_preview.iloc[idx].notna().sum()
                    if non_empty > max_non_empty:
                        max_non_empty = non_empty
                        header_row = idx
                
                df = pd.read_excel(file, sheet_name=sheet_name, header=header_row)
                
                new_columns = []
                for i, col in enumerate(df.columns):
                    col_str = str(col)
                    if 'Unnamed' in col_str or col_str.strip() == '' or col_str == 'nan':
                        new_columns.append(f"Column_{i}")
                    else:
                        new_columns.append(col_str.strip())
                
                df.columns = new_columns
                df = df.dropna(how='all')
                sheets[sheet_name] = df
            return sheets
        except Exception as e:
            st.error(f"Error reading Excel: {e}")
            return {}
    
    @staticmethod
    def extract_from_csv(file) -> Dict[str, pd.DataFrame]:
        """Extract data from CSV"""
        try:
            file.seek(0)
            df_preview = pd.read_csv(file, header=None, nrows=10)
            
            header_row = 0
            max_non_empty = 0
            
            for idx in range(min(5, len(df_preview))):
                non_empty = df_preview.iloc[idx].notna().sum()
                if non_empty > max_non_empty:
                    max_non_empty = non_empty
                    header_row = idx
            
            file.seek(0)
            df = pd.read_csv(file, header=header_row)
            
            new_columns = []
            for i, col in enumerate(df.columns):
                col_str = str(col)
                if 'Unnamed' in col_str or col_str.strip() == '' or col_str == 'nan':
                    new_columns.append(f"Column_{i}")
                else:
                    new_columns.append(col_str.strip())
            
            df.columns = new_columns
            df = df.dropna(how='all')
            
            return {"Sheet1": df}
        except Exception as e:
            st.error(f"Error reading CSV: {e}")
            return {}
    
    @staticmethod
    def extract_from_pdf(file) -> Dict[str, pd.DataFrame]:
        """Extract tables from PDF"""
        try:
            import tabula
            dfs = tabula.read_pdf(file, pages='all', multiple_tables=True)
            sheets = {}
            for i, df in enumerate(dfs):
                sheets[f"Table_{i+1}"] = df
            return sheets
        except Exception as e:
            st.warning(f"PDF extraction requires tabula-py. Attempting basic extraction...")
            return {}
    
    @staticmethod
    def extract_from_docx(file) -> Dict[str, pd.DataFrame]:
        """Extract tables from DOCX"""
        try:
            doc = Document(file)
            sheets = {}
            for i, table in enumerate(doc.tables):
                data = []
                for row in table.rows:
                    data.append([cell.text for cell in row.cells])
                if data and len(data) > 1:
                    headers = [str(h).strip() if str(h).strip() else f"Column_{j}" 
                              for j, h in enumerate(data[0])]
                    df = pd.DataFrame(data[1:], columns=headers)
                    df = df.dropna(how='all')
                    sheets[f"Table_{i+1}"] = df
            return sheets
        except Exception as e:
            st.error(f"Error reading DOCX: {e}")
            return {}

class VoucherColumnDetector:
    """Intelligently detect voucher reference columns"""
    
    @staticmethod
    def find_voucher_ref_column(df: pd.DataFrame) -> Optional[str]:
        """Find voucher reference column by various naming patterns"""
        patterns = [
            r'voucher.*ref.*no',
            r'vocher.*ref.*no',
            r'voucher.*reference.*no',
            r'vocher.*reference.*no',
            r'ref.*no',
            r'reference.*no',
            r'voucher.*ref',
            r'vocher.*ref',
            r'voucher.*reference',
            r'vocher.*reference',
            r'ref.*num',
            r'reference.*num',
            r'ref\s*no',
            r'reference\s*no',
            r'voucher\s*ref',
            r'vocher\s*ref',
            r'vr\s*no',
            r'v\.ref',
            r'doc.*ref',
            r'transaction.*ref',
            r'trans.*ref',
            r'receipt.*ref',
            r'invoice.*ref'
        ]
        
        column_scores = {}
        
        for col in df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower()
            score = 0
            
            if re.search(r'voucher\s*(no|number)', col_lower) and not re.search(r'ref', col_lower):
                continue
            
            if col_lower in ['no', 'no.', 'number', 's.no', 'sno', 'serial']:
                continue
            
            for i, pattern in enumerate(patterns):
                if re.search(pattern, col_lower):
                    score += (len(patterns) - i) * 10
                    break
            
            if 'ref' in col_lower and ('no' in col_lower or 'num' in col_lower):
                score += 20
            
            if score > 0:
                try:
                    sample_data = df[col].dropna().head(10).astype(str)
                    if len(sample_data) > 0:
                        alphanumeric_count = sum(1 for val in sample_data if bool(re.search(r'\d', val)))
                        if alphanumeric_count > len(sample_data) * 0.5:
                            score += 10
                except:
                    pass
            
            if score > 0:
                column_scores[col_str] = score
        
        if column_scores:
            best_column = max(column_scores, key=column_scores.get)
            return best_column
        
        return None
    
    @staticmethod
    def get_all_possible_columns(df: pd.DataFrame) -> List[str]:
        """Get all columns that might contain voucher or reference data"""
        possible_cols = []
        keywords = [
            'voucher', 'vocher', 'ref', 'reference', 'receipt', 
            'invoice', 'doc', 'transaction', 'trans', 'bill',
            'no', 'num', 'number', 'id', 'code'
        ]
        
        exclude_keywords = ['date', 'amount', 'total', 'quantity', 'rate', 'value', 'gstin']
        
        for col in df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower()
            
            if any(excl in col_lower for excl in exclude_keywords):
                continue
            
            if any(keyword in col_lower for keyword in keywords):
                possible_cols.append(col_str)
        
        return possible_cols
    
    @staticmethod
    def get_column_info(df: pd.DataFrame, col_name: str) -> str:
        """Get sample info about a column"""
        try:
            sample_values = df[col_name].dropna().head(5).astype(str).tolist()
            non_null_count = df[col_name].notna().sum()
            return f"Sample: {sample_values[:3]} | Non-null: {non_null_count}"
        except:
            return "Unable to fetch sample"

class Reconciliation:
    """Perform reconciliation operations"""
    
    @staticmethod
    def reconcile_sheets(sheet1: pd.DataFrame, sheet2: pd.DataFrame, 
                        ref_col1: str, ref_col2: str,
                        sheet1_name: str = "Sheet1", sheet2_name: str = "Sheet2") -> Dict:
        """Reconcile two sheets based on reference columns"""
        
        sheet1_copy = sheet1.copy()
        sheet2_copy = sheet2.copy()
        
        sheet1_copy['_ref_clean'] = sheet1_copy[ref_col1].astype(str).str.strip()
        sheet2_copy['_ref_clean'] = sheet2_copy[ref_col2].astype(str).str.strip()
        
        def is_valid_reference(val):
            if pd.isna(val) or val is None:
                return False
            val_str = str(val).strip().lower()
            if val_str in ['', 'none', 'nan', 'null', 'n/a']:
                return False
            return True
        
        sheet1_valid = sheet1_copy[sheet1_copy['_ref_clean'].apply(is_valid_reference)].copy()
        sheet2_valid = sheet2_copy[sheet2_copy['_ref_clean'].apply(is_valid_reference)].copy()
        
        s1_set = set(sheet1_valid['_ref_clean'])
        s2_set = set(sheet2_valid['_ref_clean'])
        
        matched = s1_set.intersection(s2_set)
        only_in_s1 = s1_set - s2_set
        only_in_s2 = s2_set - s1_set
        
        matched_s1 = sheet1_valid[sheet1_valid['_ref_clean'].isin(matched)].copy()
        matched_s2 = sheet2_valid[sheet2_valid['_ref_clean'].isin(matched)].copy()
        
        matched_s1 = matched_s1.sort_values('_ref_clean').reset_index(drop=True)
        matched_s2 = matched_s2.sort_values('_ref_clean').reset_index(drop=True)
        
        matched_s1_display = matched_s1.drop('_ref_clean', axis=1).copy()
        matched_s2_display = matched_s2.drop('_ref_clean', axis=1).copy()
        
        matched_s1_with_ref = matched_s1.copy()
        matched_s2_with_ref = matched_s2.copy()
        
        matched_s1_with_ref['_row_num'] = matched_s1_with_ref.groupby('_ref_clean').cumcount()
        matched_s2_with_ref['_row_num'] = matched_s2_with_ref.groupby('_ref_clean').cumcount()
        
        merged = pd.merge(
            matched_s1_with_ref,
            matched_s2_with_ref,
            on=['_ref_clean', '_row_num'],
            how='outer',
            suffixes=('_S1', '_S2')
        )
        
        merged = merged.drop(['_row_num'], axis=1)
        
        ref_col_s1 = f"{ref_col1}_S1"
        ref_col_s2 = f"{ref_col2}_S2"
        
        all_cols = merged.columns.tolist()
        
        s1_cols = [col for col in all_cols if col.endswith('_S1') and col != '_ref_clean']
        s2_cols = [col for col in all_cols if col.endswith('_S2') and col != '_ref_clean']
        
        final_cols = []
        
        if ref_col_s1 in all_cols:
            final_cols.append(ref_col_s1)
        if ref_col_s2 in all_cols:
            final_cols.append(ref_col_s2)
        
        s1_cols = [col for col in s1_cols if col not in final_cols]
        s2_cols = [col for col in s2_cols if col not in final_cols]
        
        final_cols.extend(s1_cols)
        final_cols.extend(s2_cols)
        
        if '_ref_clean' in all_cols:
            final_cols.append('_ref_clean')
        
        matched_df = merged[final_cols].copy()
        
        matched_df = matched_df.sort_values('_ref_clean').reset_index(drop=True)
        matched_df = matched_df.drop('_ref_clean', axis=1, errors='ignore')
        
        unmatched_s1 = sheet1_valid[sheet1_valid['_ref_clean'].isin(only_in_s1)].copy()
        unmatched_s1 = unmatched_s1.drop('_ref_clean', axis=1)
        unmatched_s1['Match_Status'] = f'Only in {sheet1_name}'
        
        unmatched_s2 = sheet2_valid[sheet2_valid['_ref_clean'].isin(only_in_s2)].copy()
        unmatched_s2 = unmatched_s2.drop('_ref_clean', axis=1)
        unmatched_s2['Match_Status'] = f'Only in {sheet2_name}'
        
        return {
            'matched': matched_df,
            'matched_sheet1': matched_s1_display,
            'matched_sheet2': matched_s2_display,
            'unmatched_sheet1': unmatched_s1,
            'unmatched_sheet2': unmatched_s2,
            'matched_count': len(matched),
            'unmatched_s1_count': len(only_in_s1),
            'unmatched_s2_count': len(only_in_s2),
            'matched_refs': sorted(list(matched)),
            'unmatched_refs_s1': sorted(list(only_in_s1)),
            'unmatched_refs_s2': sorted(list(only_in_s2)),
            'sheet1_name': sheet1_name,
            'sheet2_name': sheet2_name,
            'total_valid_s1': len(s1_set),
            'total_valid_s2': len(s2_set)
        }

def main():
    st.title("📊 Advanced Reconciliation Tool")
    st.markdown("---")
    
    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = {}
    if 'all_sheets' not in st.session_state:
        st.session_state.all_sheets = {}
    
    st.header("1️⃣ Upload Files")
    uploaded_files = st.file_uploader(
        "Upload files (Excel, CSV, PDF, DOCX)",
        type=['xlsx', 'xls', 'csv', 'pdf', 'docx'],
        accept_multiple_files=True,
        help="Upload multiple files for reconciliation"
    )
    
    if uploaded_files:
        extractor = DataExtractor()
        
        for uploaded_file in uploaded_files:
            file_name = uploaded_file.name
            file_ext = Path(file_name).suffix.lower()
            
            with st.spinner(f"Processing {file_name}..."):
                if file_ext in ['.xlsx', '.xls']:
                    sheets = extractor.extract_from_excel(uploaded_file)
                elif file_ext == '.csv':
                    sheets = extractor.extract_from_csv(uploaded_file)
                elif file_ext == '.pdf':
                    sheets = extractor.extract_from_pdf(uploaded_file)
                elif file_ext == '.docx':
                    sheets = extractor.extract_from_docx(uploaded_file)
                else:
                    continue
                
                for sheet_name, df in sheets.items():
                    full_name = f"{file_name} - {sheet_name}"
                    st.session_state.all_sheets[full_name] = df
        
        st.success(f"✅ Loaded {len(st.session_state.all_sheets)} sheet(s) from {len(uploaded_files)} file(s)")
    
    if st.session_state.all_sheets:
        st.header("2️⃣ Loaded Sheets")
        
        sheet_options = list(st.session_state.all_sheets.keys())
        
        with st.expander("View Loaded Sheets", expanded=False):
            for sheet_name in sheet_options:
                st.subheader(sheet_name)
                st.dataframe(st.session_state.all_sheets[sheet_name].head(10), use_container_width=True)
        
        st.header("3️⃣ Select Sheets for Reconciliation")
        
        col1, col2 = st.columns(2)
        
        with col1:
            sheet1_name = st.selectbox("Select First Sheet", sheet_options, key='sheet1')
        
        with col2:
            sheet2_options = [s for s in sheet_options if s != sheet1_name]
            sheet2_name = st.selectbox("Select Second Sheet", sheet2_options, key='sheet2')
        
        if sheet1_name and sheet2_name:
            sheet1 = st.session_state.all_sheets[sheet1_name]
            sheet2 = st.session_state.all_sheets[sheet2_name]
            
            detector = VoucherColumnDetector()
            
            auto_ref_col1 = detector.find_voucher_ref_column(sheet1)
            auto_ref_col2 = detector.find_voucher_ref_column(sheet2)
            
            st.header("4️⃣ Select Voucher Reference Columns")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(f"Sheet 1: {sheet1_name}")
                if auto_ref_col1:
                    st.success(f"✅ Auto-detected: **{auto_ref_col1}**")
                    default_idx1 = list(sheet1.columns).index(auto_ref_col1)
                else:
                    st.warning("⚠️ Could not auto-detect. Please select manually.")
                    default_idx1 = 0
                
                ref_col1 = st.selectbox(
                    "Voucher Reference Column",
                    sheet1.columns,
                    index=default_idx1,
                    key='ref1',
                    help="Select the column containing voucher reference numbers"
                )
                
                col_info = detector.get_column_info(sheet1, ref_col1)
                st.caption(f"📊 {col_info}")
                st.caption(f"Type: {sheet1[ref_col1].dtype}")
            
            with col2:
                st.subheader(f"Sheet 2: {sheet2_name}")
                if auto_ref_col2:
                    st.success(f"✅ Auto-detected: **{auto_ref_col2}**")
                    default_idx2 = list(sheet2.columns).index(auto_ref_col2)
                else:
                    st.warning("⚠️ Could not auto-detect. Please select manually.")
                    default_idx2 = 0
                
                ref_col2 = st.selectbox(
                    "Voucher Reference Column",
                    sheet2.columns,
                    index=default_idx2,
                    key='ref2',
                    help="Select the column containing voucher reference numbers"
                )
                
                col_info = detector.get_column_info(sheet2, ref_col2)
                st.caption(f"📊 {col_info}")
                st.caption(f"Type: {sheet2[ref_col2].dtype}")
            
            st.header("5️⃣ Perform Reconciliation")
            
            if st.button("🔄 Reconcile", type="primary", use_container_width=True):
                with st.spinner("Performing reconciliation..."):
                    reconciler = Reconciliation()
                    results = reconciler.reconcile_sheets(
                        sheet1, sheet2, ref_col1, ref_col2,
                        sheet1_name, sheet2_name
                    )
                    
                    st.success("✅ Reconciliation Complete!")
                    
                    st.subheader("📊 Summary Statistics")
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        st.metric("Valid References (Sheet 1)", results['total_valid_s1'])
                    
                    with col2:
                        st.metric("Valid References (Sheet 2)", results['total_valid_s2'])
                    
                    with col3:
                        st.metric("✅ Matched Records", results['matched_count'])
                    
                    with col4:
                        st.metric("❌ Unmatched (Sheet 1)", results['unmatched_s1_count'])
                    
                    with col5:
                        st.metric("❌ Unmatched (Sheet 2)", results['unmatched_s2_count'])
                    
                    st.header("📋 Reconciliation Results")
                    
                    def get_clean_name(full_name):
                        name = full_name.split(' - ')[-1] if ' - ' in full_name else full_name
                        if ' - ' in full_name:
                            parts = full_name.split(' - ')
                            if len(parts) > 1:
                                name = parts[-1]
                        return name
                    
                    clean_name1 = get_clean_name(results['sheet1_name'])
                    clean_name2 = get_clean_name(results['sheet2_name'])
                    
                    tab1, tab2, tab3 = st.tabs([
                        "✅ Matched (Side-by-Side)", 
                        f"❌ Unmatched ({clean_name1})", 
                        f"❌ Unmatched ({clean_name2})"
                    ])
                    
                    with tab1:
                        st.subheader("Matched Records - Side by Side Comparison")
                        st.info(f"Showing data from both sheets: **{clean_name1}** (columns ending with _S1) and **{clean_name2}** (columns ending with _S2)")
                        
                        if not results['matched'].empty:
                            st.dataframe(results['matched'], use_container_width=True, height=500)
                            
                            csv = results['matched'].to_csv(index=False)
                            st.download_button(
                                "📥 Download Side-by-Side Comparison",
                                csv,
                                "matched_comparison.csv",
                                "text/csv",
                                key='download-matched-comparison'
                            )
                        else:
                            st.info("No matched records found")
                    
                    with tab2:
                        st.subheader(f"Unmatched Records from {clean_name1}")
                        if not results['unmatched_sheet1'].empty:
                            st.dataframe(results['unmatched_sheet1'], use_container_width=True, height=500)
                            
                            csv = results['unmatched_sheet1'].to_csv(index=False)
                            st.download_button(
                                f"📥 Download Unmatched ({clean_name1})",
                                csv,
                                f"unmatched_{clean_name1}.csv",
                                "text/csv",
                                key='download-unmatched1'
                            )
                        else:
                            st.info(f"No unmatched records in {clean_name1}")
                    
                    with tab3:
                        st.subheader(f"Unmatched Records from {clean_name2}")
                        if not results['unmatched_sheet2'].empty:
                            st.dataframe(results['unmatched_sheet2'], use_container_width=True, height=500)
                            
                            csv = results['unmatched_sheet2'].to_csv(index=False)
                            st.download_button(
                                f"📥 Download Unmatched ({clean_name2})",
                                csv,
                                f"unmatched_{clean_name2}.csv",
                                "text/csv",
                                key='download-unmatched2'
                            )
                        else:
                            st.info(f"No unmatched records in {clean_name2}")
                    
                    with st.expander("🔍 View Voucher Reference Lists", expanded=False):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.subheader("Matched References")
                            st.write(results['matched_refs'])
                        
                        with col2:
                            st.subheader("Only in Sheet 1")
                            st.write(results['unmatched_refs_s1'])
                        
                        with col3:
                            st.subheader("Only in Sheet 2")
                            st.write(results['unmatched_refs_s2'])

if __name__ == "__main__":
    main()
