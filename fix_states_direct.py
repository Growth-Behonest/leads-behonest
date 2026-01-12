#!/usr/bin/env python3
"""
Script direto para corrigir estados vazios no CSV.
Aplica mapeamento manual para casos específicos.
"""
import pandas as pd
import re

# Mapeamento direto cidade -> UF
DIRECT_MAP = {
    # DF Regiões Administrativas
    "Águas Claras": "DF",
    "Samambaia": "DF",
    "Ceilândia": "DF",
    "Taguatinga": "DF",
    "Gama": "DF",
    "Guará": "DF",
    "Riacho Fundo 2": "DF",
    "Riacho Fundo": "DF",
    "Paranoá": "DF",
    "Brazlândia": "DF",
    "Brazlandia": "DF",
    "Sobradinho": "DF",
    "Planaltina": "DF",
    "Santa Maria": "DF",
    "Samambaia Sul": "DF",
    "Ceilândia Norte": "DF",
    "Ceilândia P Sul": "DF",
    
    # GO Entorno e Interior
    "Aparecida De Goiânia Goiás": "GO",
    "Anapolis Goiás": "GO",
    "Goiânia -": "GO",
    "Goiânia Goiás": "GO",
    "Senador Canedo Goiás": "GO",
    "Iporá Goiás": "GO",
    "Jussara Goiás": "GO",
    "Campos Belos Goiás": "GO",
    "Piranhas Goias": "GO",
    "Jaragua Goiás": "GO",
    "Alexania Goias": "GO",
    "Planaltina De Goias": "GO",
    "Novo Gama Goiás": "GO",
    "Novo Gama Lago Azul": "GO",
    "Ocidental Goiás": "GO",
    "Luziania Goiás": "GO",
    "Brasilia E Anapolis": "DF",
    "Brasília Santa Maria": "DF",
    "Samambaia Sul, Brasília": "DF",
    "Rio Verde, Goiás": "GO",
    "Rio Verde Goiás": "GO",
    "Goiânia E Rio Verde , Goias": "GO",
    "Ordália (Itauçu-Go)": "GO",
    "Apareceu": "GO",
    "Ap De Goiânia": "GO",
    "São Simão Góis": "GO",
    "São Simão Gois": "GO",
    "Pires Do Rio Góias": "GO",
    "Pires Do Rio Goias": "GO",
    "Itaberaí Goiá I": "GO",
    "Posselândia": "GO",
    "Goania": "GO",
    
    # MG
    "Coronel-Fabriciano": "MG",
    "Pouso Alegre Minas Gerais As": "MG",
    "Contagem / Conselheiro Lafaiete": "MG",
    "Patos De Monas": "MG",
    "Patos De Minas -": "MG",
    "Sete Lagoas Minas Gerais": "MG",
    "Sete Lagoas -": "MG",
    "Lagoa Santa Minas Gerais": "MG",
    "Lagoa Santa /": "MG",
    "Belo Horizonte / Nova Lima": "MG",
    "São José Da T": "MG",
    "Esmeraldas-": "MG",
    "Viçosa,": "MG",
    "Uberlândia,": "MG",
    "Uberlandi": "MG",
    "Nova Serrana/Caeté": "MG",
    "Matozinho": "MG",
    "Comtsgem": "MG",
    "Minas Gerais": "MG",
    
    # TO
    "Palmas Tocantins": "TO",
    "Palmas -": "TO",
    "Araguaína, Tocantins": "TO",
    "Nova Olinda Tocantins": "TO",
    "Santa Rita Tocantins": "TO",
    "Gurupi-": "TO",
    
    # DF / Brasília
    "Brasíli": "DF",
    "Brasília -": "DF",
    "Del Lago Itapoa": "DF",
    
    # Outros
    "Salvador -": "BA",
    "Natal-": "RN",
    "Natak": "RN",
    "Lavras -": "MG",
    "Paulista -": "PE",
    "Mineiros -": "GO",
    "Alta Floresta Mato Grosso": "MT",
    "Jequie- Bahia": "BA",
    "Pernambuco": "PE",
    
    # Inválidos / Fora do Brasil / Dados incorretos
    "Luanda": None,  # Angola
    "Junior De Souza": None,
    "Vanessa": None,
    "Anna Clara De Jesus Silveira": None,
    "27996120543": None,
    "Simoneaparecida2795@Gmail.Coma": None,
    "Odisseia": None,
    "Marajó": None,  # Ilha no PA mas dado parece inválido
    
    # Typos/variações DF
    "Sobradinholl": "DF",
    "Ceil6": "DF",
    "Águas Claras /": "DF",
}

def fix_state(row):
    """Corrige o estado baseado no mapeamento direto."""
    current = row.get("estado")
    
    # Se já tem estado válido, retorna
    if pd.notna(current) and str(current).strip() and str(current).strip() not in ["null", "NaN"]:
        return current
    
    city = str(row.get("cidade", "")).strip() if pd.notna(row.get("cidade")) else ""
    title = str(row.get("titulo", "")).strip() if pd.notna(row.get("titulo")) else ""
    
    # 1. Busca direta no mapa
    if city in DIRECT_MAP:
        result = DIRECT_MAP[city]
        return result if result else ""
    
    # 2. Busca por UF no título [Cidade/UF] ou [Cidade|UF]
    uf_match = re.search(r'[\[/|]([A-Z]{2})[\]\s]', title)
    if uf_match:
        return uf_match.group(1)
    
    # 3. Busca por nome de estado no nome da cidade
    city_lower = city.lower()
    if "goiás" in city_lower or "goias" in city_lower:
        return "GO"
    if "minas gerais" in city_lower:
        return "MG"
    if "tocantins" in city_lower:
        return "TO"
    if "bahia" in city_lower:
        return "BA"
    if "pernambuco" in city_lower:
        return "PE"
    
    # 4. Fallback para vazio ao invés de "null"
    return ""

# Main
print("Carregando CSV...")
df = pd.read_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")

def is_empty(val):
    return pd.isna(val) or str(val).strip() == "" or str(val).strip().lower() in ["null", "nan"]

empty_before = df["estado"].apply(is_empty).sum()
print(f"Vazios antes: {empty_before}")

print("Aplicando correções...")
df["estado"] = df.apply(fix_state, axis=1)

empty_after = df["estado"].apply(is_empty).sum()
print(f"Vazios depois: {empty_after}")
print(f"Corrigidos: {empty_before - empty_after}")

df.to_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
print("Salvo!")

# Mostra restantes
remaining = df[df["estado"].apply(is_empty)][["id", "cidade", "titulo"]]
if len(remaining) > 0:
    print(f"\nRestam {len(remaining)} vazios:")
    for _, row in remaining.iterrows():
        print(f"  ID {row['id']}: cidade='{row['cidade']}' titulo='{row['titulo']}'")
