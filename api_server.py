#!/usr/bin/env python3
"""
API Server para atualização do dashboard.
Expõe endpoint /api/refresh para executar o pipeline.
"""
from flask import Flask, jsonify
from flask_cors import CORS
import subprocess
import sys
import os
import threading
import time

app = Flask(__name__)
CORS(app)  # Permite requisições do frontend React

# Estado global
pipeline_status = {
    "running": False,
    "last_run": None,
    "last_success": None,
    "message": "Nenhuma atualização executada ainda"
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_pipeline_async():
    """Executa o pipeline em background."""
    global pipeline_status
    
    pipeline_status["running"] = True
    pipeline_status["message"] = "Pipeline em execução..."
    
    try:
        script_path = os.path.join(BASE_DIR, "run_pipeline.py")
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos timeout
        )
        
        pipeline_status["last_run"] = time.strftime("%d/%m/%Y %H:%M:%S")
        
        if result.returncode == 0:
            pipeline_status["last_success"] = True
            pipeline_status["message"] = "Pipeline executado com sucesso!"
        else:
            pipeline_status["last_success"] = False
            pipeline_status["message"] = f"Erro no pipeline: {result.stderr[-500:]}"
            
    except subprocess.TimeoutExpired:
        pipeline_status["last_success"] = False
        pipeline_status["message"] = "Timeout: Pipeline demorou mais de 10 minutos"
    except Exception as e:
        pipeline_status["last_success"] = False
        pipeline_status["message"] = f"Erro: {str(e)}"
    finally:
        pipeline_status["running"] = False

@app.route('/api/refresh', methods=['POST'])
def refresh():
    """Inicia o pipeline de atualização."""
    global pipeline_status
    
    if pipeline_status["running"]:
        return jsonify({
            "success": False,
            "message": "Pipeline já está em execução. Aguarde..."
        }), 409
    
    # Inicia pipeline em thread separada
    thread = threading.Thread(target=run_pipeline_async)
    thread.start()
    
    return jsonify({
        "success": True,
        "message": "Pipeline iniciado! Atualize a página em alguns minutos."
    })

@app.route('/api/status', methods=['GET'])
def status():
    """Retorna o status atual do pipeline."""
    return jsonify(pipeline_status)

@app.route('/api/health', methods=['GET'])
def health():
    """Health check."""
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    print("🚀 Iniciando API Server na porta 5001...")
    print("   Endpoints disponíveis:")
    print("   - POST /api/refresh - Executa o pipeline")
    print("   - GET /api/status   - Status do pipeline")
    print("   - GET /api/health   - Health check")
    app.run(host='0.0.0.0', port=5001, debug=False)
