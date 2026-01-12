#!/usr/bin/env python3
"""
Adiciona coluna 'classificacao_index' ao CSV baseada no score_index.
Classificações:
- 6.00 a 4.09: MQL+
- 4.09 a 3.58: MQL
- 3.58 a 3.00: LEAD+ (tem grana)
- 3.00 a 0.62: LEAD
- 0.62 a 0.00: DESQUALIFICADO 100%
"""
import pandas as pd

def parse_br_decimal(value):
    """Converte string BR para float: '0,75' -> 0.75"""
    if pd.isna(value) or value == "":
        return 0.0
    return float(str(value).replace(",", "."))

def classify(score):
    """Classifica o lead baseado no score_index."""
    if score >= 4.09:
        return "MQL+"
    elif score >= 3.58:
        return "MQL"
    elif score >= 3.0:
        return "LEAD+"
    elif score >= 0.62:
        return "LEAD"
    else:
        return "DESQUALIFICADO 100%"

# Main
print("Carregando CSV...")
df = pd.read_csv("leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")

print("Classificando leads baseado no score_index...")
df["_score_float"] = df["score_index"].apply(parse_br_decimal)
df["classificacao_index"] = df["_score_float"].apply(classify)

# Remove coluna temporária
df = df.drop(columns=["_score_float"])

# Estatísticas
print("\nDistribuição de classificações:")
counts = df["classificacao_index"].value_counts()
for classif in ["MQL+", "MQL", "LEAD+", "LEAD", "DESQUALIFICADO 100%"]:
    if classif in counts:
        count = counts[classif]
        pct = count / len(df) * 100
        print(f"  {classif}: {count} ({pct:.1f}%)")

# Salva
df.to_csv("leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
print("\nCSV atualizado com coluna 'classificacao_index'!")

# Amostra
print("\nAmostra por classificação:")
for classif in ["MQL+", "MQL", "LEAD+", "LEAD", "DESQUALIFICADO 100%"]:
    sample = df[df["classificacao_index"] == classif][["id", "nome", "score_index", "classificacao_index"]].head(3)
    if len(sample) > 0:
        print(f"\n{classif}:")
        print(sample.to_string())
