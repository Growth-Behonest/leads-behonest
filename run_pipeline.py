#!/usr/bin/env python3
"""
Pipeline completo de processamento de leads.
Executa todos os scripts de ETL em sequência.
"""
import asyncio
import subprocess
import sys
import os
import shutil
from datetime import datetime

# Diretório base do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "leads-dashboard/public/leads_sults_consolidado.csv")
DASHBOARD_PUBLIC = os.path.join(BASE_DIR, "leads-dashboard", "public")

def run_script(script_name, description):
    """Executa um script Python e retorna sucesso/falha."""
    script_path = os.path.join(BASE_DIR, script_name)
    print(f"\n{'='*60}")
    print(f"📊 {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.returncode == 0:
            print(f"✅ {script_name} executado com sucesso!")
            # Mostra output resumido
            lines = result.stdout.strip().split('\n')
            for line in lines[-10:]:  # Últimas 10 linhas
                print(f"   {line}")
            return True
        else:
            print(f"❌ Erro em {script_name}:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout ao executar {script_name}")
        return False
    except Exception as e:
        print(f"❌ Exceção ao executar {script_name}: {e}")
        return False

async def run_extract_script():
    """Executa o script de extração assíncrono."""
    print(f"\n{'='*60}")
    print(f"📊 Passo 1: Extraindo dados da API SULTS...")
    print(f"{'='*60}")
    
    # Importa e executa diretamente para manter o async
    sys.path.insert(0, BASE_DIR)
    
    try:
        import extract_sults_data
        await extract_sults_data.main_corrected()
        print("✅ Extração concluída!")
        return True
    except Exception as e:
        print(f"❌ Erro na extração: {e}")
        return False

def copy_to_dashboard():
    """Copia o CSV para a pasta public do dashboard."""
    print(f"\n{'='*60}")
    print(f"📦 Copiando CSV para o Dashboard...")
    print(f"{'='*60}")
    
    try:
        dest = os.path.join(DASHBOARD_PUBLIC, "leads-dashboard/public/leads_sults_consolidado.csv")
        shutil.copy2(CSV_FILE, dest)
        print(f"✅ CSV copiado para {dest}")
        return True
    except Exception as e:
        print(f"❌ Erro ao copiar: {e}")
        return False

async def run_full_pipeline():
    """Executa o pipeline completo."""
    start_time = datetime.now()
    
    print("\n" + "🚀" * 30)
    print("   INICIANDO PIPELINE DE PROCESSAMENTO DE LEADS")
    print("🚀" * 30)
    print(f"   Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    
    steps = [
        # Passo 1: Extração (async)
        ("extract", "Extraindo dados da API SULTS"),
        # Passos 2-7: Scripts síncronos
        ("add_location_index.py", "Passo 2: Calculando índice de localização"),
        ("add_investimento_index.py", "Passo 3: Calculando índice de investimento"),
        ("add_tempo_index.py", "Passo 4: Calculando índice de tempo"),
        ("add_score_index.py", "Passo 5: Calculando score final"),
        ("add_classificacao_index.py", "Passo 6: Classificando leads"),
        ("fix_origem_facebook.py", "Passo 7: Corrigindo origem Facebook → Meta Ads"),
        ("sync_supabase.py", "Passo 8: Sincronizando com Supabase"),
    ]
    
    success = True
    
    for script, description in steps:
        if script == "extract":
            # Extração assíncrona
            result = await run_extract_script()
        else:
            # Scripts síncronos
            result = run_script(script, description)
        
        if not result:
            success = False
            print(f"\n❌ Pipeline interrompido devido a erro em: {script}")
            break
    
    # Passo final: Copiar para dashboard
    if success:
        success = copy_to_dashboard()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
    else:
        print("❌ PIPELINE CONCLUÍDO COM ERROS!")
    print(f"   Duração: {duration:.1f} segundos")
    print(f"   Término: {end_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)
    
    return success

def main():
    """Ponto de entrada principal."""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    success = asyncio.run(run_full_pipeline())
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
