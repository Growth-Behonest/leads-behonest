#!/usr/bin/env python3
"""
Corrige coluna 'localizacao_index' no CSV.
Regras:
- Estado DEVE ser MG, DF ou GO (outros estados = 0)
- Cidade DEVE estar na lista de franquias (outras cidades = 0)
- DF inteiro = 1 (Distrito Federal é a franquia toda)
"""
import pandas as pd
import unicodedata

def normalize(text):
    """Remove acentos e converte para minúsculas."""
    if not text or pd.isna(text):
        return ""
    return unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

# Estados válidos
VALID_STATES = {"MG", "DF", "GO"}

# Cidades de franquias (normalizadas)
FRANCHISE_CITIES_MG = {
    "belo horizonte", "betim", "contagem", "nova lima", "pocos de caldas",
    "pouso alegre", "governador valadares", "ipatinga", "paracatu", "sabara",
    "sarzedo", "ibirite", "igarape", "pedro leopoldo", "vespasiano",
    "ribeirao das neves", "divinopolis", "itabirito", "brumadinho",
    "para de minas", "patos de minas",
    # Cidades emergentes MG
    "esmeraldas", "barbacena", "bom despacho"
}

FRANCHISE_CITIES_GO = {
    "anapolis", "aparecida de goiania", "goiania"
}

def calculate_location_index(row):
    """Retorna 1 se lead está na área de franquias, 0 caso contrário."""
    estado = str(row.get("estado", "")).strip().upper() if pd.notna(row.get("estado")) else ""
    cidade = normalize(row.get("cidade", ""))
    
    # 1. Se estado não é MG, DF ou GO = 0
    if estado not in VALID_STATES:
        return 0
    
    # 2. Se DF = 1 (todo o Distrito Federal é franquia)
    if estado == "DF":
        return 1
    
    # 3. Se GO, verifica se cidade está na lista GO
    if estado == "GO":
        if cidade in FRANCHISE_CITIES_GO:
            return 1
        # Match parcial para variações (ex: "Goiânia -" -> "goiania")
        for fc in FRANCHISE_CITIES_GO:
            if fc in cidade or cidade in fc:
                if len(fc) >= 5:
                    return 1
        return 0
    
    # 4. Se MG, verifica se cidade está na lista MG
    if estado == "MG":
        if cidade in FRANCHISE_CITIES_MG:
            return 1
        # Match parcial para variações
        for fc in FRANCHISE_CITIES_MG:
            if fc in cidade or cidade in fc:
                if len(fc) >= 5:
                    return 1
        return 0
    
    return 0

# Main
print("Carregando CSV...")
df = pd.read_csv("leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")

print("Recalculando localizacao_index...")
df["localizacao_index"] = df.apply(calculate_location_index, axis=1)

# Estatísticas
total = len(df)
dentro = (df["localizacao_index"] == 1).sum()
fora = (df["localizacao_index"] == 0).sum()

print(f"\nResultados:")
print(f"  Total de leads: {total}")
print(f"  Dentro da área de franquias (1): {dentro} ({dentro/total*100:.1f}%)")
print(f"  Fora da área (0): {fora} ({fora/total*100:.1f}%)")

# Breakdown por estado
print("\nBreakdown por estado:")
for estado in ["MG", "DF", "GO"]:
    subset = df[df["estado"] == estado]
    if len(subset) > 0:
        ones = (subset["localizacao_index"] == 1).sum()
        print(f"  {estado}: {ones}/{len(subset)} leads com index=1")

# Mostra exemplos de estados fora (devem ter 0)
print("\nExemplos de estados fora (devem ser 0):")
other_states = df[~df["estado"].isin(VALID_STATES)][["id", "cidade", "estado", "localizacao_index"]].head(5)
print(other_states.to_string())

# Salva
df.to_csv("leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
print("\nCSV atualizado!")

# Recalcula score_index também
print("\nRecalculando score_index com a nova localizacao_index...")
