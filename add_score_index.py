#!/usr/bin/env python3
"""
Adiciona coluna 'score_index' ao CSV.
Fórmula: (x * y) + z
  x = localizacao_index
  y = investimento_index
  z = tempo_index
"""
import pandas as pd

def parse_br_decimal(value):
    """Converte string BR para float: '0,75' -> 0.75"""
    if pd.isna(value) or value == "":
        return 0.0
    return float(str(value).replace(",", "."))

def format_br(value):
    """Formata float para string BR: 0.75 -> '0,75'"""
    return str(round(value, 2)).replace(".", ",")

# Main
print("Carregando CSV...")
df = pd.read_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")

print("Calculando score_index = (localizacao_index * 3) + (investimento_index * 2) + (tempo_index * 0,5)")

# Converte para float
df["_x"] = df["localizacao_index"].apply(lambda v: float(v) if pd.notna(v) else 0.0)
df["_y"] = df["investimento_index"].apply(parse_br_decimal)
df["_z"] = df["tempo_index"].apply(parse_br_decimal)

# Fórmula: (x*3) + (y*2) + (z*0.5)
df["_score"] = (df["_x"] * 3) + (df["_y"] * 2) + (df["_z"] * 0.5)

# Formata para BR
df["score_index"] = df["_score"].apply(format_br)

# Remove colunas temporárias
df = df.drop(columns=["_x", "_y", "_z", "_score"])

# Estatísticas
scores = df["score_index"].apply(parse_br_decimal)
print(f"\nEstatísticas score_index:")
print(f"  Mínimo: {format_br(scores.min())}")
print(f"  Máximo: {format_br(scores.max())}")
print(f"  Média: {format_br(scores.mean())}")

# Salva
df.to_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
print("\nCSV atualizado com coluna 'score_index'!")

# Top 10 leads
print("\nTop 10 leads por score_index:")
df["_score_float"] = df["score_index"].apply(parse_br_decimal)
top10 = df.nlargest(10, "_score_float")[["id", "nome", "localizacao_index", "investimento_index", "tempo_index", "score_index"]]
print(top10.to_string())
