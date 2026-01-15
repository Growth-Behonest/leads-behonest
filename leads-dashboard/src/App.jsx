import { useState, useMemo, useEffect, useCallback } from 'react';
import { runClientPipeline } from './utils/pipeline';
import { supabase } from './services/supabase';
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
    etapaFunil: [],
    dataInicio: '2023-11-09',
    dataFim: '2026-01-07'
  });

  const handleLogin = () => {
    localStorage.setItem('auth_token', 'verified');
    setIsAuthenticated(true);
  };



  // ...

  // Função para carregar do Supabase
  const loadFromSupabase = useCallback(async () => {
    setLoading(true);
    try {
      // Busca paginada: carrega em lotes de 500 para evitar timeout
      const BATCH_SIZE = 500;
      let allLeads = [];
      let offset = 0;
      let hasMore = true;

      while (hasMore) {
        const { data, error } = await supabase
          .from('leads')
          .select('*')
          .order('id', { ascending: false })
          .range(offset, offset + BATCH_SIZE - 1);

        if (error) throw error;

        if (data && data.length > 0) {
          allLeads = [...allLeads, ...data];
          offset += BATCH_SIZE;

          // Se veio menos que o batch, acabou
          if (data.length < BATCH_SIZE) {
            hasMore = false;
          }
        } else {
          hasMore = false;
        }
      }

      if (allLeads.length > 0) {
        setLeads(allLeads);

        // Recalcula max date
        let maxDateStr = '2026-01-07';
        let maxDateIso = null;

        allLeads.forEach(lead => {
          if (lead.data_criacao) {
            const parts = lead.data_criacao.split('/');
            if (parts.length === 3) {
              const iso = `${parts[2]}-${parts[1]}-${parts[0]}`;
              if (!maxDateIso || iso > maxDateIso) {
                maxDateIso = iso;
                maxDateStr = iso;
              }
            }
          }
        });
        setFilters(prev => ({ ...prev, dataFim: maxDateStr }));
      }
    } catch (err) {
      console.error("Erro ao carregar do Supabase:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  // Carrega na inicialização
  useEffect(() => {
    if (isAuthenticated) {
      loadFromSupabase();
    }
  }, [loadFromSupabase, isAuthenticated]);

  // ... (filters/stats useMemos remain roughly same)

  // Função para atualizar os dados (chama o pipeline + sync Supabase)
  const handleRefresh = async () => {
    if (refreshing) return;

    setRefreshing(true);
    setRefreshStatus({ type: 'info', message: 'Iniciando atualização dos dados... (Isso pode demorar alguns minutos)' });

    // Token SULTS do .env
    const token = import.meta.env.VITE_SULTS_API_TOKEN;
    if (!token) {
      setRefreshStatus({ type: 'error', message: 'Token SULTS não encontrado.' });
      setRefreshing(false);
      return;
    }

    try {
      const onProgress = (msg) => {
        setRefreshStatus({ type: 'info', message: msg });
      };

      const newLeads = await runClientPipeline(token, onProgress);

      if (newLeads && newLeads.length > 0) {
        setLeads(newLeads);

        // Sincroniza com Supabase
        onProgress("Salvando dados no Supabase...");

        // Split in batches
        const BATCH_SIZE = 100;
        for (let i = 0; i < newLeads.length; i += BATCH_SIZE) {
          const batch = newLeads.slice(i, i + BATCH_SIZE);
          const { error } = await supabase.from('leads').upsert(batch);
          if (error) {
            console.error("Erro no batch Supabase:", error);
            // Continue or break?
            // throw error; // maybe better to log and continue or partial fail
          }
        }

        // Recalculates Max Date filter
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

        setRefreshStatus({ type: 'success', message: '✅ Dados atualizados e salvos no Supabase!' });

      } else {
        setRefreshStatus({ type: 'error', message: 'Nenhum lead encontrado.' });
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
      estados: getUnique('estado'),
      etapasFunil: getUnique('etapa_funil')
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
        const valorStr = String(lead.valor_disponivel_para_investimento || '0');
        const valorLead = parseFloat(valorStr.replace(/\./g, '').replace(',', '.') || 0);
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

      // Filtro por etapa do funil (Multi)
      if (filters.etapaFunil.length > 0 && !filters.etapaFunil.includes(lead.etapa_funil)) {
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
        <div id="dashboard" className="charts-grid">
          <DonutChart data={filteredLeads} />
          <OriginChart data={filteredLeads} />
        </div>

        {/* Gráfico Classificação x Motivo de Perda */}
        <ClassificationLossChart data={filteredLeads} />

        {/* Tabela de Leads */}
        <div id="leads">
          <LeadsTable leads={filteredLeads} />
        </div>
      </main>
    </>
  );
}

export default App;
