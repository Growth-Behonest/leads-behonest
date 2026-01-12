import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
import re
import logging
import sys
import time
import ssl
import certifi

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================
# Insira seu Token SULTS aqui
API_TOKEN = "O2JlaG9uZXN0YnJhc2lsOzE3NTI3ODQ3ODg1OTY="
BASE_URL = "https://api.sults.com.br/api/v1/expansao"
OUTPUT_FILE = "leads_sults_consolidado.csv"

# Configurações de Concorrência e Retry
MAX_CONCURRENT_REQUESTS = 10  # Limite de requisições simultâneas
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # Segundos

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# FUNÇÕES DE EXTRAÇÃO (E da ETL)
# ==============================================================================

def get_headers():
    """Retorna os headers padrão para autenticação."""
    return {
        "Authorization": API_TOKEN, 
        "Content-Type": "application/json;charset=UTF-8"
}

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, params: dict = None) -> dict:
    """Faz uma requisição GET com mecanismo de retry exponencial."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate Limit
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Rate limit atingido em {url}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Erro {response.status} ao acessar {url}: {await response.text()}")
                    # Em caso de erro 500 ou outros, tenta novamente? 
                    # Se for 4xx (exceto 429), provavelmente não adianta tentar de novo
                    if 400 <= response.status < 500:
                        return None
                    await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Exceção na requisição para {url}: {e}")
            await asyncio.sleep(RETRY_DELAY)
    return None

async def fetch_all_leads(session: aiohttp.ClientSession) -> list:
    """Extrai TODOS os leads iterando pelas páginas."""
    logger.info("Iniciando extração de Leads (Master Data)...")
    all_leads = []
    page = 0
    limit = 100
    
    while True:
        params = {"start": page, "limit": limit}
        url = f"{BASE_URL}/negocio"
        
        logger.info(f"Baixando página {page}...")
        data = await fetch_with_retry(session, url, params)
        
        if not data or "data" not in data or not data["data"]:
            logger.info("Fim da paginação ou retorno vazio.")
            break
            
        leads_page = data["data"]
        
        # Filtra (Client-Side) apenas Funil ID = 1 (Franqueado)
        # Estrutura: lead -> etapa -> funil -> id
        filtered_page = []
        for lead in leads_page:
            try:
                funil_id = lead.get("etapa", {}).get("funil", {}).get("id")
                if funil_id == 1:
                    filtered_page.append(lead)
            except Exception:
                continue
                
        all_leads.extend(filtered_page)
        
        total_page = data.get("totalPage", 0)
        
        # Se 'start' for índice de página, incrementamos +1
        # Se a resposta indicar que chegamos na última página, paramos
        # A API retorna "totalPage". Se page (indice 0) + 1 >= totalPage, paramos?
        # Vamos assumir incremento seguro.
        if (page + 1) >= total_page:
            break
            
        page += 1
        
    logger.info(f"Total de Leads extraídos: {len(all_leads)}")
    return all_leads

async def fetch_pipeline_timeline(session: aiohttp.ClientSession, lead_id: int, semaphore: asyncio.Semaphore) -> float:
    """
    Busca a timeline de um lead específico e extrai o valor de investimento.
    Utiliza semáforo para controlar concorrência.
    """
    url = f"{BASE_URL}/negocio/{lead_id}/timeline"
    investment_value = 0.0
    
    async with semaphore:  # Limita concorrência aqui
        data = await fetch_with_retry(session, url)
        
    if data and "data" in data:
        investment_value = parse_investment_from_timeline(data["data"])
        
    return investment_value

# ==============================================================================
# FUNÇÕES DE TRANSFORMAÇÃO (T da ETL)
# ==============================================================================

def format_date_br(date_str):
    """Converte ISO (YYYY-MM-DDTHH:MM:SSZ) para DD/MM/YYYY."""
    try:
        if not date_str:
            return ""
        # Pega apenas a data (YYYY-MM-DD)
        date_part = date_str.split("T")[0]
        year, month, day = date_part.split("-")
        return f"{day}/{month}/{year}"
    except Exception:
        return date_str

import unicodedata

def normalize_text(text: str) -> str:
    """Remove acentos e converte para minúsculas."""
    if not text:
        return ""
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

async def load_ibge_data(session):
    """
    Baixa lista de municípios do IBGE e retorna dict {nome_normalizado: UF}.
    """
    try:
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        logger.info("Baixando dados do IBGE para mapeamento de cidades...")
        async with session.get(url, timeout=30) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Cria mapa: { "belo horizonte": "MG", "goiania": "GO" ... }
                # Normaliza a chave para facilitar busca
                city_map = {}
                for item in data:
                     name = item.get("nome", "")
                     # Safe access: (item.get("key") or {}).get("key")
                     micro = item.get("microrregiao") or {}
                     meso = micro.get("mesorregiao") or {}
                     uf_obj = meso.get("UF") or {}
                     uf = uf_obj.get("sigla", "")
                     
                     if name and uf:
                         city_map[normalize_text(name)] = uf
                logger.info(f"Mapeamento IBGE carregado: {len(city_map)} cidades.")
                return city_map
            else:
                logger.error(f"Erro ao baixar dados IBGE: Status {resp.status}")
                return {}
    except Exception as e:
        logger.error(f"Erro ao carregar dados IBGE: {e}")
        return {}

def get_state_from_text(text: str, ibge_map: dict = None) -> str:
    """
    Busca menções de estados ou cidades no texto (ex: Título).
    Retorna UF ou None.
    """
    if not text:
        return None
        
    text_normalized = normalize_text(text)
    
    # 1. Busca por siglas de estado explícitas
    states = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
    
    for uf in states:
        uf_lower = uf.lower()
        # Patterns: " mg ", "/mg", "-mg", "(mg)"
        patterns = [
            f" {uf_lower} ", f"/{uf_lower}", f"-{uf_lower}", f"({uf_lower})", f" {uf_lower}$"
        ]
        for p in patterns:
            if p in text_normalized:
                return uf
                
    # 2. Busca no Mapa IBGE (se fornecido)
    # Isso pode ser lento se o texto for longo e iterarmos tudo.
    # Vamos buscar apenas palavras chave se possível ou keywords comuns
    # Melhor estratégia: Se tiver keywords manuais, ok. Varredura completa é pesada.
    
    # Busca simplificada por principais capitais
    common_cities = {
        "belo horizonte": "MG", "bh": "MG",
        "brasília": "DF", "brasilia": "DF", "df": "DF",
        "goiânia": "GO", "goiania": "GO",
        "são paulo": "SP", "sp": "SP",
        "rio de janeiro": "RJ", "rj": "RJ"
    }
    
    for city, uf in common_cities.items():
        if city in text_normalized:
            return uf
            
    return None

def get_state_from_city(city_name, existing_uf, ibge_map=None):
    """
    Inferência de Estado (UF) e Normalização de Cidade.
    Usa mapa do IBGE se disponível.
    """
    if not city_name:
        return city_name, existing_uf
    
    # 1. Normalização Inicial
    city_clean = city_name.strip()
    
    # Remove sufixos comuns de UF na cidade (Ex: "Brasília Df" -> "Brasília")
    city_clean = re.sub(r'\s+(df|mg|go|sp|rj|pr|sc|rs|ba|pe|ce|am|pa|to|ma|pi|rn|pb|al|se|es|ms|mt|ac|rr|ap|ro)$', '', city_clean, flags=re.IGNORECASE).strip()
    city_clean = re.sub(r'[\/\-]\s*(df|mg|go|sp|rj|pr|sc|rs|ba|pe|ce|am|pa|to|ma|pi|rn|pb|al|se|es|ms|mt|ac|rr|ap|ro)$', '', city_clean, flags=re.IGNORECASE).strip()

    city_key = normalize_text(city_clean)
    
    # Aliases
    aliases = {
        "bh": "Belo Horizonte",
        "belo horizonte": "Belo Horizonte",
        "df": "Brasília",
        "bsb": "Brasília",
        "gyn": "Goiânia",
        "goiania": "Goiânia",
        "sp": "São Paulo",
        "rj": "Rio de Janeiro"
    }
    
    # Resolve Alias
    if city_key in aliases:
        # Recupera o nome formatado do alias se quiser, ou mantém o limpo
        real_name = aliases[city_key]
        city_clean = real_name # Ex: "BH" -> "Belo Horizonte"
        # Re-normaliza a chave para buscar UF correta do alias se não tiver no ibge map com esse nome exato?
        # Mas "Belo Horizonte" estará no IBGE map.
        city_key = normalize_text(real_name)
    else:
        # Title Case para bonito
        city_clean = city_clean.title()
        
    # 2. Mapeamento UF
    final_uf = existing_uf
    
    if not final_uf and ibge_map:
        # Tenta buscar no mapa IBGE
        # A chave do mapa IBGE é normalizada (sem acento, minuscula)
        if city_key in ibge_map:
            final_uf = ibge_map[city_key]
            
    # Fallback para mapa manual se IBGE falhar ou não carregou
    if not final_uf:
         # ... (mantemos o mapa manual anterior ou simplificado como fallback?)
         # IBGE tem tudo. Se não achou lá, é difícil achar no manual, exceto erros de digitação.
         pass
         
    return city_clean, final_uf

def parse_investment_from_timeline(timeline_items: list) -> float:
    """
    Analisa os itens da timeline (HTML) em busca do 'Valor de Investimento'.
    Retorna o valor como float ou 0.0 se não encontrar.
    """
    for item in timeline_items:
        html_contents = []
        
        if "descricaoHtml" in item:
            html_contents.append(item["descricaoHtml"])
        
        if "atividade" in item and "descricaoHtml" in item["atividade"]:
            html_contents.append(item["atividade"]["descricaoHtml"])
            
        if "anotacao" in item and "descricaoHtml" in item["anotacao"]:
            html_contents.append(item["anotacao"]["descricaoHtml"])

        if "checkpoint" in item and "descricaoHtml" in item["checkpoint"]:
            html_contents.append(item["checkpoint"]["descricaoHtml"])

        for html_text in html_contents:
            if not html_text:
                continue
                
            soup = BeautifulSoup(html_text, 'html.parser')
            text_content = soup.get_text(" ", strip=True) 
            
            text_lower = text_content.lower()
            
            # Procura por termos chave
            if "investimento" in text_lower or "valor disponivel" in text_lower or "capital" in text_lower:
                # Regex aprimorada para capturar formatos como: 
                # 60.000, 120.000, R$ 200.000, 60.000 a 120.000, 60 a 120 mil
                
                try:
                    # Encontra índice da keyword
                    idx = -1
                    for key in ["investimento", "valor", "capital"]:
                        idx = text_lower.find(key)
                        if idx != -1:
                            break
                    
                    if idx != -1:
                        # Pega o texto subsequente (ex: próximos 50 caracteres)
                        sub_text = text_content[idx:idx+80] # "Investimento: 60 a 120 mil URL..."
                        
                        # Regex para capturar número e opcionalmente o sufixo "mil"
                        # Padrão: (numero) (espaço opcional) (sujeira opcional) (mil)?
                        # Ex: 60.000 | 60 a 120 mil | 200 mil
                        
                        # Procura o primeiro número
                        # match group 1: numero
                        # checagem manual de sufixo "mil" após o numero
                        
                        matches = re.finditer(r'([\d\.,]+)', sub_text)
                        
                        for m in matches:
                            val_str = m.group(1)
                            val = clean_currency_string(val_str)
                            
                            # Verifica contexto posterior para "mil"
                            end_pos = m.end()
                            suffix = sub_text[end_pos:end_pos+20].lower() # " a 120 mil URL..."
                            
                            # Se tiver "mil" logo depois (ignorando " a 120" se for faixa)
                            # Caso "60 a 120 mil" -> o "mil" se aplica ao 120, mas logicamente se aplica ao 60 também?
                            # Geralmente sim. "Investimento de 60 a 120 mil".
                            # Se encontrarmos "mil" no sufixo, assumimos que é milhar?
                            
                            if "mil" in suffix or "mi " in suffix:
                                # Se o valor for pequeno (<1000), multiplica
                                if val < 1000:
                                    val *= 1000
                            
                            if val >= 1000: # Aceita 1000 ou mais como investimento válido
                                return val
                            
                            # Caso especial: Se for 60 e tiver " a 120 mil", o "mil" ta longe.
                            # Mas se o range for "60 a 120 mil", o 60 tb é mil.
                            # Regex para pegar faixa: (\d+) a (\d+) mil
                            range_match = re.search(r'([\d\.,]+)\s*a\s*([\d\.,]+)\s*mil', sub_text, re.IGNORECASE)
                            if range_match:
                                v1_str = range_match.group(1)
                                v1 = clean_currency_string(v1_str)
                                if v1 < 1000:
                                    v1 *= 1000
                                return v1
                                    
                except Exception:
                    pass

    return 0.0

def clean_currency_string(value_str: str) -> float:
    """
    Converte string de moeda brasileira (60.000,00 ou 60.000) para float.
    CORREÇÃO: 60.000 deve ser 60000.0, não 60.0
    """
    try:
        # Remove tudo que não é digito, ponto ou virgula
        clean = re.sub(r'[^\d,\.]', '', value_str)
        # Remove ponto no final ou virgula no final soltos
        clean = clean.strip(".,")
        
        if not clean:
            return 0.0

        # Caso 1: Tem virgula e ponto (1.000,00) -> Padrão BR
        if ',' in clean and '.' in clean:
            clean = clean.replace('.', '') # Tira milhar
            clean = clean.replace(',', '.') # Virgula vira ponto decimal
            
        # Caso 2: Tem apenas Ponto (60.000 ou 60.00)
        elif '.' in clean:
            # Se tiver 3 casas decimais (100.000), é milhar
            # Se tiver 2 casas (100.00), pode ser decimal US ou milhar incompleto?
            # Assumindo contexto PT-BR do CRM SULTS: Ponto quase sempre é milhar.
            # Risco: 60.5 (60 e meio) vs 60.500 (sessenta mil e quinhentos).
            parts = clean.split('.')
            if len(parts) > 1 and len(parts[-1]) == 3:
                # 60.000 -> remove ponto
                clean = clean.replace('.', '')
            elif len(parts) > 1 and len(parts[-1]) == 2:
                # 60.00 -> decimal
                 pass # Já está em formato float python
            else:
                 # 1.000.000 -> remove pontos
                 if clean.count('.') >= 1:
                     clean = clean.replace('.', '')

        # Caso 3: Tem apenas Virgula (60,00 ou 1000,00)
        elif ',' in clean:
            clean = clean.replace(',', '.')

        return float(clean)
    except ValueError:
        return 0.0

def format_currency_br(val: float) -> str:
    """Formata float para string BRL (1.000,00)."""
    if val == 0.0:
        return "0,00"
    return f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ==============================================================================
# MAIN ASYNC
# ==============================================================================

async def main():
    # Placeholder para compatibilidade se algo chamar main()
    await main_corrected()

def is_invalid_phone(phone: str) -> bool:
    """Retorna True se o telefone for composto apenas por dígitos repetidos (ex: 999999999)."""
    if not phone:
        return False
    digits = re.sub(r"\D", "", str(phone))
    if not digits:
        return False
    # Verifica se todos os dígitos são iguais
    return len(set(digits)) == 1



async def main_corrected():
    logger.info("=== Iniciando Script de Extração SULTS ===")
    
    # Configuração SSL Segura com Certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(headers=get_headers(), connector=connector) as session:
        # 0. Carrega Mapa IBGE
        ibge_map = await load_ibge_data(session)

        # 1. Fetches Leads
        leads = await fetch_all_leads(session)
        if not leads:
            return

        # 2. Fetches Timelines
        logger.info(f"Extraindo details (N+1) para {len(leads)} leads...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        async def fetch_and_enrich(lead):
            # Filtros pré-request
            # 1. Filtro IDs específicos
            lead_id = lead.get("id")
            if lead_id in [7286, 4918, 2067, 2090]:
                return None
                
            # 2. Filtro de Texto (Nome, Email, Titulo contendo "teste")
            search_text = f"{lead.get('nome') or ''} {lead.get('email') or ''} {lead.get('titulo') or ''}".lower()
            if "teste" in search_text:
                return None
                
            # 3. Filtro de Telefone Inválido (Dígitos repetidos)
            phone = lead.get("contatoPessoa")[0].get("phone") if lead.get("contatoPessoa") else None
            if phone and is_invalid_phone(phone):
                return None
                
            # 4. Filtro de Origem "DUPLICADO" ou "TESTE"
            origem_obj = lead.get("origem") or {}
            origem_nome = origem_obj.get("nome", "")
            if origem_nome:
                origem_upper = origem_nome.upper()
                if "DUPLICADO" in origem_upper or "TESTE" == origem_upper:
                    return None

            async with semaphore:
                url = f"{BASE_URL}/negocio/{lead.get('id')}/timeline"
                data = await fetch_with_retry(session, url)
                val_float = 0.0
                if data and "data" in data:
                    val_float = parse_investment_from_timeline(data["data"])
                
                # Formatações
                city_raw = lead.get("cidade")
                uf_raw = lead.get("uf")
                # get_state_from_city retorna (city_clean, uf_final)
                city_clean, uf_final = get_state_from_city(city_raw, uf_raw, ibge_map)
                
                # Fallback: Se UF continua vazia, tenta buscar no Título do Negócio
                if not uf_final:
                    uf_final = get_state_from_text(lead.get("titulo"), ibge_map)
                    
                # Final Fallback: "null" string
                if not uf_final:
                    uf_final = "null"
                    
                val_formatted = format_currency_br(val_float)
                
                # Extração aprimorada do Motivo de Perda
                motivo_nome = lead.get("situacaoPerdaMotivo", {}).get("nome") if lead.get("situacaoPerdaMotivo") else ""
                motivo_obs = lead.get("situacaoPerdaMotivoObservacao") or ""
                motivo_final = motivo_nome
                if motivo_obs:
                    if motivo_final:
                        motivo_final += f" - {motivo_obs}"
                    else:
                        motivo_final = motivo_obs

                return {
                    "id": lead.get("id"),
                    "data_criacao": format_date_br(lead.get("dtCadastro")),
                    "titulo": lead.get("titulo"),
                    "nome": lead.get("contatoPessoa")[0].get("nome") if lead.get("contatoPessoa") else None,
                    "email": lead.get("contatoPessoa")[0].get("email") if lead.get("contatoPessoa") else None,
                    "celular": lead.get("contatoPessoa")[0].get("phone") if lead.get("contatoPessoa") else None,
                    "origem": lead.get("origem", {}).get("nome") if lead.get("origem") else None,
                    "cidade": city_clean, # Usando cidade normalizada
                    "estado": uf_final,
                    "etiquetas": ", ".join([t["nome"] for t in (lead.get("etiqueta") or [])]),
                    "situacao": lead.get("situacao", {}).get("nome") if lead.get("situacao") else None,
                    "motivo_perda": motivo_final,
                    "valor_disponivel_para_investimento": val_formatted
                }

        tasks = []
        ids_to_ignore = [7286, 4918, 2067, 2090]
        
        for lead in leads:
            # 1. Filtro de ID Manual
            if lead.get("id") in ids_to_ignore:
                continue

            # 2. Filtro de Teste (Nome, Email, Titulo)
            contact = lead.get("contatoPessoa")[0] if lead.get("contatoPessoa") else {}
            name = contact.get("nome", "").lower()
            email = contact.get("email", "").lower()
            title = lead.get("titulo", "").lower()
            
            if "teste" in name or "teste" in email or "teste" in title:
                continue
                
            # 3. Filtro de Celular Inválido (Digitos iguais)
            phone = contact.get("phone", "")
            if phone:
                digits = re.sub(r'\D', '', phone)
                # Se tiver digitos e todos forem iguais (ex: 999999999)
                if digits and re.match(r'^(\d)\1+$', digits):
                    continue

            # 4. Filtro de Origem "DUPLICADO" ou "TESTE"
            # Precisamos checar a origem antes de processar
            origem_obj = lead.get("origem") or {}
            origem_nome = origem_obj.get("nome", "")
            if origem_nome:
                origem_upper = origem_nome.upper()
                if "DUPLICADO" in origem_upper or "TESTE" == origem_upper:
                    continue
                
            tasks.append(fetch_and_enrich(lead))
        
        # Executa tasks e mostra progresso
        results = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processando")]
        
    # 3. Exportação
    df = pd.DataFrame(results)
    
    # 5. Deduplicação Final por Celular
    if not df.empty and "celular" in df.columns:
        # Cria coluna temp limpa para deduplicar
        df["_cel_clean"] = df["celular"].astype(str).apply(lambda x: re.sub(r'\D', '', x))
        # Remove onde clean é vazio (opcional, mas seguro manter leads sem telefone?)
        # O usuário disse "confira pela coluna celular se existem celulares iguais".
        # Leads sem telefone (vazio) seriam considerados iguais entre si.
        # Vamos assumir que duplicados vazios também devem ser removidos (mantendo um).
        
        df = df.drop_duplicates(subset=["_cel_clean"], keep="first")
        df = df.drop(columns=["_cel_clean"])

    # Tratamento final de nulos no valor
    
    # Tratamento final de nulos no valor (caso tenha escapado, embora format_currency_br trate)
    df["valor_disponivel_para_investimento"] = df["valor_disponivel_para_investimento"].fillna("0,00")
    
    logger.info(f"Salvando arquivo em: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig", sep=";")
    logger.info("Processo concluído com sucesso!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_corrected())
