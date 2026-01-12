#!/usr/bin/env python3
"""
Script para corrigir estados 'null' no CSV consolidado.
Aplica regras de normalização avançada e mapeamento manual.
"""

import pandas as pd
import re
import unicodedata

def normalize(text):
    """Remove acentos e converte para minúsculas."""
    if not text or pd.isna(text):
        return ""
    return unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

# Mapeamento manual de bairros/regiões administrativas do DF
DF_NEIGHBORHOODS = {
    "aguas claras", "aguasclaras", "taguatinga", "ceilandia", "samambaia",
    "gama", "guara", "riacho fundo", "riacho fundo 2", "paranoa", "brazlandia",
    "sobradinho", "sobradinholl", "sobradinho ii", "planaltina", "santa maria",
    "recanto das emas", "sao sebastiao", "itapoa", "del lago itapoa",
    "samambaia sul", "ceilandia norte", "ceilandia p sul", "ceil6",
    "nucleo bandeirante", "candangolandia", "lago sul", "lago norte",
    "sudoeste", "octogonal", "cruzeiro", "asa sul", "asa norte", "varjao"
}

# Mapeamento de cidades do Entorno de Brasília (GO)
ENTORNO_GO = {
    "valparaiso", "valparaiso de goias", "novo gama", "novo gama goias",
    "cidade ocidental", "ocidental goias", "luziania", "luziania goias",
    "aguas lindas", "aguas lindas de goias", "planaltina de goias",
    "santo antonio do descoberto", "formosa", "cristalina", "alexania",
    "alexania goias", "padre bernardo", "abadiania", "posselanda", "posselandia"
}

# Correções de typos conhecidos
TYPO_FIXES = {
    "brasili": "brasilia",
    "brasilia -": "brasilia",
    "goania": "goiania",
    "goiania -": "goiania",
    "uberlandi": "uberlandia",
    "anapolis goias": "anapolis",
    "goiania goias": "goiania",
    "aparecida de goiania goias": "aparecida de goiania",
    "senador canedo goias": "senador canedo",
    "rio verde goias": "rio verde",
    "rio verde, goias": "rio verde",
    "jaragua goias": "jaragua",
    "campos belos goias": "campos belos",
    "piranhas goias": "piranhas",
    "jussara goias": "jussara",
    "ipora goias": "ipora",
    "pires do rio goias": "pires do rio",
    "pires do rio goias": "pires do rio",
    "novo gama lago azul": "novo gama",
    "sete lagoas minas gerais": "sete lagoas",
    "sete lagoas -": "sete lagoas",
    "lagoa santa minas gerais": "lagoa santa",
    "lagoa santa /": "lagoa santa",
    "contagem / conselheiro lafaiete": "contagem",
    "belo horizonte / nova lima": "belo horizonte",
    "patos de monas": "patos de minas",
    "patos de minas -": "patos de minas",
    "palmas tocantins": "palmas",
    "palmas -": "palmas",
    "araguaina, tocantins": "araguaina",
    "nova olinda tocantins": "nova olinda",
    "santa rita tocantins": "santa rita do tocantins",
    "gurupi-": "gurupi",
    "salvador -": "salvador",
    "natal-": "natal",
    "lavras -": "lavras",
    "vicosa,": "vicosa",
    "esmeraldas-": "esmeraldas",
    "uberlandia,": "uberlandia",
    "coronel-fabriciano": "coronel fabriciano",
    "alta floresta mato grosso": "alta floresta",
    "nova serrana/caete": "nova serrana",
    "paulista -": "paulista",
    "mineiros -": "mineiros",
    "comtsgem": "contagem",
    "matozinho": "matozinhos",
    "natak": "natal",
    "ap de goiania": "aparecida de goiania",
    "apareceu": "aparecida de goiania",
    "goiania e rio verde , goias": "goiania",
    "brasilia e anapolis": "brasilia",
    "brasilia santa maria": "brasilia",
    "samambaia sul, brasilia": "brasilia",
    "planaltina de goias": "planaltina de goias",  # GO, não DF
    "sao simao gois": "sao simao",
    "itaberai goia i": "itaberai",
    "pouso alegre minas gerais as": "pouso alegre",
}

# Mapeamento cidade -> UF (fallback manual para casos problemáticos)
CITY_STATE_MAP = {
    # Minas Gerais
    "coronel fabriciano": "MG",
    "matozinhos": "MG",
    "viçosa": "MG",
    "vicosa": "MG",
    "lavras": "MG",
    "pouso alegre": "MG",
    "nova serrana": "MG",
    "caete": "MG",
    "esmeraldas": "MG",
    "sao jose da lapa": "MG",
    "governador valadares": "MG",
    
    # Goiás
    "anapolis": "GO",
    "itaberai": "GO",
    "jaragua": "GO", 
    "campos belos": "GO",
    "piranhas": "GO",
    "jussara": "GO",
    "ipora": "GO",
    "pires do rio": "GO",
    "sao simao": "GO",
    "mineiros": "GO",
    "planaltina de goias": "GO",
    
    # Tocantins
    "palmas": "TO",
    "araguaina": "TO",
    "gurupi": "TO",
    "nova olinda": "TO",
    "santa rita do tocantins": "TO",
    
    # Outros
    "natal": "RN",
    "paulista": "PE",
    "jequie": "BA",
    "alta floresta": "MT",
    "luanda": None,  # Angola, não Brasil
}

def infer_state(row):
    """Infere o estado baseado na cidade e título."""
    current_state = row.get("estado")
    # Check if state is already valid (not null, not empty, not NaN)
    if current_state and str(current_state).strip() and str(current_state).strip().lower() != "null":
        return current_state
    
    city = str(row.get("cidade", "")).strip()
    title = str(row.get("titulo", "")).strip()
    
    if not city and not title:
        return "null"
    
    # Normalize
    city_norm = normalize(city)
    title_norm = normalize(title)
    
    # 1. Aplica correções de typo
    for typo, fix in TYPO_FIXES.items():
        if typo in city_norm:
            city_norm = city_norm.replace(typo, fix)
            break
    
    # Remove sufixos de estado embutidos no nome da cidade
    state_suffixes = [
        " goias", " minas gerais", " tocantins", " bahia", " mato grosso",
        " pernambuco", " rio grande do norte", " parana", " santa catarina",
        " rio grande do sul", " sao paulo", " rio de janeiro", " espirito santo"
    ]
    for suffix in state_suffixes:
        if city_norm.endswith(suffix):
            city_norm = city_norm[:-len(suffix)].strip()
    
    # Remove trailing punctuation
    city_norm = re.sub(r'[\s\-,/]+$', '', city_norm).strip()
    
    # 2. Verifica se é bairro/RA do DF
    if city_norm in DF_NEIGHBORHOODS:
        return "DF"
        
    # 3. Verifica se é cidade do Entorno de Brasília (GO)
    if city_norm in ENTORNO_GO:
        return "GO"
    
    # 4. Busca no mapa manual
    if city_norm in CITY_STATE_MAP:
        state = CITY_STATE_MAP[city_norm]
        if state:
            return state
    
    # 5. Busca UF no título
    # Formato comum: [Cidade/UF] ou [Cidade|UF]
    uf_match = re.search(r'[/|]([A-Z]{2})\]', title)
    if uf_match:
        return uf_match.group(1)
    
    # Padrões como "Cidade/UF" ou "Cidade - UF"
    uf_match2 = re.search(r'[\s\-/]([A-Z]{2})(?:\s|$|\])', title)
    if uf_match2:
        uf = uf_match2.group(1)
        valid_ufs = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
        if uf in valid_ufs:
            return uf
    
    # 6. Se cidade contém nome de estado, extrai
    if "minas gerais" in city_norm or "mg" in city.lower().split():
        return "MG"
    if "goias" in city_norm or ("go" in city.lower().split() and "hugo" not in city_norm):
        return "GO"
    if "tocantins" in city_norm:
        return "TO"
    if "bahia" in city_norm:
        return "BA"
    if "pernambuco" in city_norm:
        return "PE"
    
    return "null"

def main():
    print("Carregando CSV...")
    df = pd.read_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", encoding="utf-8-sig")
    
    print(f"Total de linhas: {len(df)}")
    
    # Conta nulls antes (NaN + empty string + "null")
    def is_empty_state(val):
        if pd.isna(val):
            return True
        if str(val).strip() == "" or str(val).strip().lower() == "null":
            return True
        return False
    
    nulls_before = df["estado"].apply(is_empty_state).sum()
    print(f"Estados vazios/null antes: {nulls_before}")
    
    # Aplica inferência
    print("Aplicando correções...")
    df["estado"] = df.apply(infer_state, axis=1)
    
    # Conta nulls depois
    nulls_after = df["estado"].apply(is_empty_state).sum()
    print(f"Estados vazios/null depois: {nulls_after}")
    print(f"Corrigidos: {nulls_before - nulls_after}")
    
    # Salva
    df.to_csv("leads-dashboard/public/leads_sults_consolidado.csv", sep=";", index=False, encoding="utf-8-sig")
    print("CSV atualizado com sucesso!")
    
    # Lista restantes com null
    remaining = df[df["estado"].apply(is_empty_state)][["id", "cidade", "titulo", "estado"]]
    if len(remaining) > 0:
        print(f"\nRestam {len(remaining)} leads com estado vazio:")
        print(remaining.head(20).to_string())

if __name__ == "__main__":
    main()
