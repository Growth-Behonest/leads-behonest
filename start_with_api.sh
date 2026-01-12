#!/bin/bash
# Script para iniciar a API backend e o frontend React

echo "🚀 Iniciando sistema de Dashboard de Leads..."

# Diretório base
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Função para matar processos ao sair
cleanup() {
    echo ""
    echo "🛑 Encerrando servidores..."
    kill $API_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    exit 0
}

trap cleanup SIGINT SIGTERM

# Inicia a API Backend
echo "📡 Iniciando API Backend (porta 5001)..."
cd "$BASE_DIR"
source venv/bin/activate 2>/dev/null || true
python api_server.py &
API_PID=$!

# Aguarda um pouco
sleep 2

# Inicia o Frontend React
echo "🖥️  Iniciando Frontend React (porta 5173)..."
cd "$BASE_DIR/leads-dashboard"
npm run dev &
FRONTEND_PID=$!

echo ""
echo "✅ Servidores iniciados!"
echo "   📡 API Backend: http://localhost:5001"
echo "   🖥️  Frontend:    http://localhost:5173"
echo ""
echo "Pressione Ctrl+C para encerrar ambos os servidores."
echo ""

# Aguarda
wait
