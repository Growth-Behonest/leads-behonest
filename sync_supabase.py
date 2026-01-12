import os
import pandas as pd
from supabase import create_client, Client

# Supabase Credentials (retrieved dynamically)
SUPABASE_URL = "https://hkchcyigfklqjqmkllrl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhrY2hjeWlnZmtscWpxbWtsbHJsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODIxNzMyNywiZXhwIjoyMDgzNzkzMzI3fQ.-WJkjechoEu4PZZU0K0q7j7lonDWHlxE7ISUd1Mu28M"

import math

def clean_currency(value):
    if pd.isna(value) or value == '' or str(value).strip().lower() in ['nan', 'none', 'null']:
        return None  # Send null to DB
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
             return None
        return float(value)
    # Remove dots (thousand separators) and replace comma with dot (decimal separator)
    # Format "150.000,00" -> "150000.00"
    cleaned = str(value).replace('.', '').replace(',', '.')
    try:
        f = float(cleaned)
        if math.isnan(f) or math.isinf(f):
             return None
        return f
    except ValueError:
        return None

def clean_float(value):
    if pd.isna(value) or value == '' or str(value).strip().lower() in ['nan', 'none', 'null']:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
             return None
        return float(value)
    cleaned = str(value).replace(',', '.')
    try:
        f = float(cleaned)
        if math.isnan(f) or math.isinf(f):
             return None
        return f
    except ValueError:
        return None

import json

def sync_leads():
    print("Connecting to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    csv_path = 'leads-dashboard/public/leads_sults_consolidado.csv'
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    print(f"Reading {csv_path}...")
    # keep_default_na=False prevents pandas from interpreting "NaN", "null" etc as floats
    df = pd.read_csv(csv_path, sep=';', dtype=str, keep_default_na=False) 
    
    print(f"Processing {len(df)} rows...")
    
    records = []
    for i, row in df.iterrows():
        try:
            # Handle empty strings that pandas might read as ''
            def get_val(col):
                v = row.get(col, '')
                return v.strip() if isinstance(v, str) else str(v)

            # ID check
            raw_id = get_val('id')
            if not raw_id or not raw_id.isdigit():
                continue
                
            record = {
                "id": int(raw_id),
                "data_criacao": get_val('data_criacao'),
                "titulo": get_val('titulo'),
                "nome": get_val('nome'),
                "email": get_val('email'),
                "celular": get_val('celular'),
                "origem": get_val('origem'),
                "cidade": get_val('cidade'),
                "estado": get_val('estado'),
                "etiquetas": get_val('etiquetas'),
                "situacao": get_val('situacao'),
                "motivo_perda": get_val('motivo_perda'),
                "valor_disponivel_para_investimento": clean_currency(get_val('valor_disponivel_para_investimento')),
                "localizacao_index": clean_float(get_val('localizacao_index')),
                "investimento_index": clean_float(get_val('investimento_index')),
                "tempo_index": clean_float(get_val('tempo_index')),
                "score_index": clean_float(get_val('score_index')),
                "classificacao_index": get_val('classificacao_index')
            }
            
            records.append(record)
        except Exception as e:
            print(f"Skipping row {i} due to error: {e}, Row: {row.values}")

    # Validate JSON compliance before sending
    # Filter out any records that still have bad floats (just in case)
    valid_records = []
    for r in records:
        try:
             # Test serialization
             json.dumps(r) 
             valid_records.append(r)
        except Exception as e:
             print(f"Record failed JSON validation: {e}. Record: {r}")

    records = valid_records

    # Upsert data in batches
    batch_size = 100
    print(f"Upserting {len(records)} records in batches of {batch_size}...")
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            response = supabase.table('leads').upsert(batch).execute()
        except Exception as e:
            print(f"Error upserting batch {i//batch_size}: {e}")
            break

    print("Sync complete.")

if __name__ == "__main__":
    sync_leads()
