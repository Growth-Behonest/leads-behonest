#!/usr/bin/env python3
"""
Corrige coluna 'tempo_index' para formato brasileiro (vírgula como separador decimal).
Valores de 0,0 a 1,0.
"""
import pandas as pd
from datetime import datetime

# Data de referência (hoje)
TODAY = datetime(2026, 1, 7)

def parse_date(date_str):
    """Converte DD/MM/YYYY para datetime."""
    if pd.isna(date_str) or not date_str:
        return None
    try:
        return datetime.strptime(str(date_str).strip(), "%d/%m/%Y")
    except:
        return None

# Main
print("Carregando CSV...")
df = pd.read_csv("leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")

print("Parseando datas...")
df["_parsed_date"] = df["data_criacao"].apply(parse_date)

# Calcula dias desde criação
df["_days_ago"] = df["_parsed_date"].apply(
    lambda d: (TODAY - d).days if d else None
)

# Encontra range de dias
valid_days = df["_days_ago"].dropna()
max_days = valid_days.max()
min_days = valid_days.min()

print(f"Lead mais recente: {min_days} dias atrás")
print(f"Lead mais antigo: {max_days} dias atrás")

# Calcula tempo_index entre 0 e 1
def calculate_tempo_index(days_ago):
    if pd.isna(days_ago):
        return 0.0
    
    if max_days == min_days:
        return 1.0
    
    # Normalização: (max - atual) / (max - min)
    index = (max_days - days_ago) / (max_days - min_days)
    return round(index, 2)  # 2 casas decimais

df["tempo_index_float"] = df["_days_ago"].apply(calculate_tempo_index)

# Formata para string com vírgula (formato brasileiro): "0,52"
def format_br(value):
    return str(round(value, 2)).replace(".", ",")

df["tempo_index"] = df["tempo_index_float"].apply(format_br)

# Remove colunas temporárias
df = df.drop(columns=["_parsed_date", "_days_ago", "tempo_index_float"])

# Estatísticas
print(f"\nExemplos de tempo_index:")
for _, row in df.head(10).iterrows():
    print(f"  {row['data_criacao']} -> {row['tempo_index']}")

# Salva
df.to_csv("leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
print("\nCSV atualizado!")

# Amostra: leads mais recentes
print("\nLeads mais recentes:")
recent = df[df["tempo_index"].isin(["1,0", "0,99", "0,98"])][["id", "data_criacao", "nome", "tempo_index"]].head(5)
print(recent.to_string())

print("\nLeads mais antigos:")
old = df[df["tempo_index"].isin(["0,0", "0,01", "0,02"])][["id", "data_criacao", "nome", "tempo_index"]].head(5)
print(old.to_string())
