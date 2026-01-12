#!/usr/bin/env python3
"""
Adiciona coluna 'investimento_index' ao CSV.
Valor decimal de 0 a 1 baseado no valor de investimento.
- 0 = 0
- 200.000 = 1
- Valores acima de 200k também = 1 (cap)
"""
import pandas as pd
import re

MAX_INVESTMENT = 200000.0  # 200 mil = índice 1,0

def parse_br_currency(value):
    """
    Converte formato brasileiro para float.
    "60.000,00" -> 60000.0
    "0" -> 0.0
    "0,00" -> 0.0
    """
    if pd.isna(value) or value == "" or value == "0":
        return 0.0
    
    # Remove espaços
    s = str(value).strip()
    
    # Se for só número sem formatação
    if s.isdigit():
        return float(s)
    
    # Remove pontos de milhar e troca vírgula por ponto
    # "60.000,00" -> "60000.00"
    s = s.replace(".", "").replace(",", ".")
    
    try:
        return float(s)
    except:
        return 0.0

def format_br(value):
    """Formata float para string BR com vírgula."""
    return str(round(value, 2)).replace(".", ",")

# Main
print("Carregando CSV...")
df = pd.read_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")

print("Parseando valores de investimento...")
df["_invest_float"] = df["valor_disponivel_para_investimento"].apply(parse_br_currency)

# Estatísticas
print(f"\nInvestimentos:")
print(f"  Mínimo: R$ {df['_invest_float'].min():,.0f}")
print(f"  Máximo: R$ {df['_invest_float'].max():,.0f}")
print(f"  Média: R$ {df['_invest_float'].mean():,.0f}")

# Calcula índice: valor / 200000, com cap em 1.0
def calculate_invest_index(value):
    if value <= 0:
        return 0.0
    index = value / MAX_INVESTMENT
    return min(index, 1.0)  # Cap em 1.0

df["_invest_index_float"] = df["_invest_float"].apply(calculate_invest_index)

# Formata para BR
df["investimento_index"] = df["_invest_index_float"].apply(format_br)

# Remove colunas temporárias
df = df.drop(columns=["_invest_float", "_invest_index_float"])

# Exemplos
print("\nExemplos de investimento_index:")
print("  R$ 0 -> 0")
print("  R$ 60.000 -> 0,3")
print("  R$ 120.000 -> 0,6")
print("  R$ 150.000 -> 0,75")
print("  R$ 200.000+ -> 1,0")

# Salva
df.to_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
print("\nCSV atualizado com coluna 'investimento_index'!")

# Amostra
print("\nAmostra de dados:")
sample = df[["id", "valor_disponivel_para_investimento", "investimento_index"]].head(20)
print(sample.to_string())
