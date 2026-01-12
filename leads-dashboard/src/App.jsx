import { useState, useMemo, useEffect, useCallback } from 'react';
import Papa from 'papaparse';
import Header from './components/Header';
import Filters from './components/Filters';
import LeadsTable from './components/LeadsTable';
import DonutChart from './components/DonutChart';
import OriginChart from './components/OriginChart';
import ClassificationLossChart from './components/ClassificationLossChart';
import './App.css';

import Login from './components/Login';

// URL da API backend
const API_URL = 'http://localhost:5001';

function App() {
  // Estado de Autenticação
  const [isAuthenticated, setIsAuthenticated] = useState(() => {
    return localStorage.getItem('auth_token') === 'verified';
  });

  const [leads, setLeads] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshStatus, setRefreshStatus] = useState(null);
  const [filters, setFilters] = useState({
    etiqueta: [],
    situacao: [],
    nome: '',
    valor: '',
    classificacao: [],
    origem: [],
    estado: [],
    dataInicio: '2023-11-09',
    dataFim: '2026-01-07'
  });

  const handleLogin = () => {
    localStorage.setItem('auth_token', 'verified');
    setIsAuthenticated(true);
  };

  // Função para carregar o CSV
  const loadCSV = useCallback(() => {
    fetch('/leads_sults_consolidado.csv?t=' + Date.now()) // Cache bust
      .then(response => response.text())
      .then(csvText => {
        Papa.parse(csvText, {
          header: true,
          delimiter: ';',
          skipEmptyLines: true,
          complete: (results) => {
            setLeads(results.data);

            // Encontra a data mais recente
            let maxDate = null;
            let maxDateStr = '2026-01-07'; // Fallback

            results.data.forEach(lead => {
              if (lead.data_criacao) {
                // Formato esperado: dd/mm/yyyy
                const parts = lead.data_criacao.split('/');
                if (parts.length === 3) {
                  // Cria data para comparação (yyyy-mm-dd)
                  // Nota: lead.data_criacao está em dd/mm/yyyy
                  const isoDate = `${parts[2]}-${parts[1]}-${parts[0]}`;

                  if (!maxDate || isoDate > maxDate) {
                    maxDate = isoDate;
                    maxDateStr = isoDate;
                  }
                }
              }
            });

            setFilters(prev => ({ ...prev, dataFim: maxDateStr }));
            setLoading(false);
          }
        });
      })
      .catch(err => {
        console.error('Erro ao carregar CSV:', err);
        setLoading(false);
      });
  }, []);

  // Carrega o CSV na inicialização
  useEffect(() => {
    if (isAuthenticated) {
      loadCSV();
    }
  }, [loadCSV, isAuthenticated]);

  // Se não estiver autenticado, mostra login
  if (!isAuthenticated) {
    return <Login onLogin={handleLogin} />;
  }

  // Função para atualizar os dados (chama o pipeline)
  const handleRefresh = async () => {
    if (refreshing) return;

    setRefreshing(true);
    setRefreshStatus({ type: 'info', message: 'Iniciando atualização dos dados...' });

    try {
      const response = await fetch(`${API_URL}/api/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const data = await response.json();

      if (response.ok) {
        setRefreshStatus({ type: 'info', message: 'Pipeline iniciado! Aguarde alguns minutos...' });

        // Polling do status
        const checkStatus = async () => {
          try {
            const statusRes = await fetch(`${API_URL}/api/status`);
            const statusData = await statusRes.json();

            if (!statusData.running) {
              // Pipeline terminou
              if (statusData.last_success) {
                setRefreshStatus({ type: 'success', message: '✅ Dados atualizados com sucesso!' });
                // Recarrega os dados
                setTimeout(() => {
                  loadCSV();
                  setRefreshStatus(null);
                }, 2000);
              } else {
                setRefreshStatus({ type: 'error', message: `❌ ${statusData.message}` });
              }
              setRefreshing(false);
              return;
            }

            // Ainda executando, verifica novamente em 5 segundos
            setTimeout(checkStatus, 5000);
          } catch (err) {
            console.error('Erro ao verificar status:', err);
            setTimeout(checkStatus, 5000);
          }
        };

        // Inicia polling após 3 segundos
        setTimeout(checkStatus, 3000);

      } else {
        setRefreshStatus({ type: 'error', message: data.message || 'Erro ao iniciar atualização' });
        setRefreshing(false);
      }
    } catch (err) {
      console.error('Erro ao chamar API:', err);
      setRefreshStatus({
        type: 'error',
        message: 'Erro de conexão com o servidor. Verifique se a API está rodando (porta 5001).'
      });
      setRefreshing(false);
    }
  };

  // Extrai opções únicas para os filtros
  const filterOptions = useMemo(() => {
    const getUnique = (field) => {
      const values = new Set();
      leads.forEach(lead => {
        if (lead[field]) {
          if (field === 'etiquetas') {
            lead[field].split(',').forEach(tag => {
              const trimmed = tag.trim();
              if (trimmed) values.add(trimmed);
            });
          } else {
            values.add(lead[field].trim());
          }
        }
      });
      return [...values].sort();
    };

    return {
      etiquetas: getUnique('etiquetas'),
      situacoes: getUnique('situacao'),
      classificacoes: getUnique('classificacao_index'),
      origens: getUnique('origem'),
      estados: getUnique('estado')
    };
  }, [leads]);

  // Filtra os leads
  const filteredLeads = useMemo(() => {
    return leads.filter(lead => {
      // Filtro por etiqueta (Multi)
      if (filters.etiqueta.length > 0) {
        if (!lead.etiquetas) return false;
        const leadTags = lead.etiquetas.split(',').map(t => t.trim());
        const hasMatch = filters.etiqueta.some(selectedTag => leadTags.includes(selectedTag));
        if (!hasMatch) return false;
      }

      // Filtro por situação (Multi)
      if (filters.situacao.length > 0 && !filters.situacao.includes(lead.situacao)) {
        return false;
      }

      // Filtro por nome
      if (filters.nome && !lead.nome?.toLowerCase().includes(filters.nome.toLowerCase())) {
        return false;
      }

      // Filtro por valor disponível
      if (filters.valor) {
        const valorLead = parseFloat(lead.valor_disponivel_para_investimento?.replace('.', '').replace(',', '.') || 0);
        const valorFiltro = parseFloat(filters.valor);
        if (valorLead !== valorFiltro) return false;
      }

      // Filtro por classificação (Multi)
      if (filters.classificacao.length > 0 && !filters.classificacao.includes(lead.classificacao_index)) {
        return false;
      }

      // Filtro por origem (Multi)
      if (filters.origem.length > 0 && !filters.origem.includes(lead.origem)) {
        return false;
      }

      // Filtro por estado (Multi)
      if (filters.estado.length > 0 && !filters.estado.includes(lead.estado)) {
        return false;
      }

      // Filtro por data
      if (filters.dataInicio || filters.dataFim) {
        const parseDate = (dateStr) => {
          if (!dateStr) return null;
          const parts = dateStr.split('/');
          if (parts.length === 3) {
            return new Date(parts[2], parts[1] - 1, parts[0]);
          }
          return new Date(dateStr);
        };

        const leadDate = parseDate(lead.data_criacao);
        if (leadDate) {
          if (filters.dataInicio) {
            const startDate = new Date(filters.dataInicio);
            if (leadDate < startDate) return false;
          }
          if (filters.dataFim) {
            const endDate = new Date(filters.dataFim);
            if (leadDate > endDate) return false;
          }
        }
      }

      return true;
    });
  }, [leads, filters]);

  // Estatísticas
  const stats = useMemo(() => {
    const total = filteredLeads.length;
    const ganhos = filteredLeads.filter(l => l.situacao === 'GANHO').length;
    const perdas = filteredLeads.filter(l => l.situacao === 'PERDA').length;
    const andamento = filteredLeads.filter(l => l.situacao === 'ANDAMENTO').length;

    return { total, ganhos, perdas, andamento };
  }, [filteredLeads]);

  // Função de exportar CSV
  const exportToCSV = () => {
    if (filteredLeads.length === 0) {
      alert('Não há dados para exportar');
      return;
    }

    // Converte para CSV usando PapaParse
    const csv = Papa.unparse(filteredLeads, {
      delimiter: ';',
      header: true
    });

    // Cria blob e faz download
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `leads_filtrados_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <>
        <Header />
        <div className="app-container">
          <div className="loading">
            <div className="loading-spinner"></div>
          </div>
        </div>
      </>
    );
  }

  return (
    <>
      <Header />
      <main className="app-container">
        <div className="page-header">
          <h1 className="page-title">Dashboard de Leads</h1>
          <div className="header-actions">
            <button
              className={`btn-refresh ${refreshing ? 'refreshing' : ''}`}
              onClick={handleRefresh}
              disabled={refreshing}
            >
              {refreshing ? '🔄 Atualizando...' : '🔄 Atualizar Dados'}
            </button>
            <button className="btn-export" onClick={exportToCSV}>
              ⬇ Exportar CSV ({stats.total})
            </button>
          </div>
        </div>

        {/* Status de Refresh */}
        {refreshStatus && (
          <div className={`refresh-status refresh-status-${refreshStatus.type}`}>
            {refreshStatus.message}
          </div>
        )}

        {/* Estatísticas */}
        <div className="stats-container">
          <div className="stat-card">
            <div className="stat-value">{stats.total}</div>
            <div className="stat-label">Total Leads</div>
          </div>
          <div className="stat-card">
            <div className="stat-value" style={{ color: '#51cf66' }}>{stats.ganhos}</div>
            <div className="stat-label">Ganhos</div>
          </div>
          <div className="stat-card">
            <div className="stat-value" style={{ color: '#ff6b6b' }}>{stats.perdas}</div>
            <div className="stat-label">Perdas</div>
          </div>
          <div className="stat-card">
            <div className="stat-value" style={{ color: '#ffc107' }}>{stats.andamento}</div>
            <div className="stat-label">Em Andamento</div>
          </div>
        </div>

        {/* Filtros */}
        <Filters
          filters={filters}
          setFilters={setFilters}
          options={filterOptions}
        />

        {/* Grid de Gráficos */}
        <div className="charts-grid">
          <DonutChart data={filteredLeads} />
          <OriginChart data={filteredLeads} />
        </div>

        {/* Gráfico Classificação x Motivo de Perda */}
        <ClassificationLossChart data={filteredLeads} />

        {/* Tabela de Leads */}
        <LeadsTable leads={filteredLeads} />
      </main>
    </>
  );
}

export default App;
