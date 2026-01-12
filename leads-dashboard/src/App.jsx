import { useState, useMemo, useEffect, useCallback } from 'react';
import { runClientPipeline } from './utils/pipeline';
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
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5001';

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

  // Se não estiver autenticado, mostra login
  if (!isAuthenticated) {
    return <Login onLogin={handleLogin} />;
  }



  // ... (other code)

  // Função para atualizar os dados (chama o pipeline client-side)
  const handleRefresh = async () => {
    if (refreshing) return;

    setRefreshing(true);
    setRefreshStatus({ type: 'info', message: 'Iniciando atualização dos dados... (Isso pode demorar alguns minutos)' });

    // Token SULTS do .env
    const token = import.meta.env.VITE_SULTS_API_TOKEN;
    if (!token) {
      setRefreshStatus({ type: 'error', message: 'Token da API SULTS não configurado no .env (VITE_SULTS_API_TOKEN)' });
      setRefreshing(false);
      return;
    }

    try {
      // Callback para atualizar status na UI
      const onProgress = (msg) => {
        setRefreshStatus({ type: 'info', message: msg });
      };

      const newLeads = await runClientPipeline(token, onProgress);

      if (newLeads && newLeads.length > 0) {
        setLeads(newLeads);

        // Atualiza filtro de data
        // Recalcula max date
        // (Copy logic from loadCSV or make a helper? Just simple max search here)
        let maxDateStr = filters.dataFim;
        let maxDateIso = null;

        newLeads.forEach(l => {
          const parts = l.data_criacao.split("/");
          if (parts.length === 3) {
            const iso = `${parts[2]}-${parts[1]}-${parts[0]}`;
            if (!maxDateIso || iso > maxDateIso) {
              maxDateIso = iso;
              maxDateStr = iso;
            }
          }
        });
        setFilters(prev => ({ ...prev, dataFim: maxDateStr }));

        setRefreshStatus({ type: 'success', message: `✅ Atualizado! ${newLeads.length} leads carregados.` });

        // Opcional: Baixar CSV novo automaticamente?
        // alert("Dados atualizados! O novo CSV não foi salvo no servidor (Vercel é estático). Use o botão de Exportar para salvar seus dados.");
      } else {
        setRefreshStatus({ type: 'error', message: 'Nenhum lead encontrado ou erro na extração.' });
      }
    } catch (err) {
      console.error('Erro na pipeline:', err);
      setRefreshStatus({
        type: 'error',
        message: `Erro: ${err.message}`
      });
    } finally {
      setRefreshing(false);
    }
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
