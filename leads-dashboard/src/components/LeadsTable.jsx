import React, { useState } from 'react';

function LeadsTable({ leads }) {
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 15;

    const totalPages = Math.ceil(leads.length / itemsPerPage);
    const startIndex = (currentPage - 1) * itemsPerPage;
    const currentLeads = leads.slice(startIndex, startIndex + itemsPerPage);

    const formatCurrency = (value) => {
        if (!value || value === '0,00') return 'R$ 0,00';
        const numValue = parseFloat(value.replace('.', '').replace(',', '.'));
        return new Intl.NumberFormat('pt-BR', {
            style: 'currency',
            currency: 'BRL'
        }).format(numValue);
    };

    const getStatusClass = (situacao) => {
        const sit = situacao?.toUpperCase();
        if (sit === 'PERDA') return 'status-badge status-perda';
        if (sit === 'GANHO') return 'status-badge status-ganho';
        if (sit === 'ANDAMENTO') return 'status-badge status-andamento';
        return 'status-badge';
    };

    const getClassificacaoClass = (classificacao) => {
        const cls = classificacao?.toUpperCase() || '';
        if (cls.includes('MQL')) return 'tag tag-mql';
        if (cls.includes('DESQUALIFICADO')) return 'tag tag-desqualificado';
        return 'tag tag-lead';
    };

    if (leads.length === 0) {
        return (
            <div className="card fade-in">
                <div className="no-results">
                    <div className="no-results-icon">📋</div>
                    <p>Nenhum lead encontrado com os filtros aplicados.</p>
                </div>
            </div>
        );
    }

    return (
        <div className="card fade-in">
            <h2 className="card-title">Leads ({leads.length} resultados)</h2>
            <div className="table-container">
                <table className="leads-table">
                    <thead>
                        <tr>
                            <th>Etiquetas</th>
                            <th>Situação</th>
                            <th>Nome</th>
                            <th>Valor Disponível</th>
                            <th>Classificação</th>
                            <th>Origem</th>
                            <th>Estado</th>
                            <th>Data</th>
                        </tr>
                    </thead>
                    <tbody>
                        {currentLeads.map((lead, idx) => (
                            <tr key={lead.id || idx}>
                                <td>
                                    {lead.etiquetas?.split(',').slice(0, 2).map((tag, i) => (
                                        <span key={i} className="tag tag-lead" style={{ marginRight: '4px' }}>
                                            {tag.trim()}
                                        </span>
                                    ))}
                                </td>
                                <td>
                                    <span className={getStatusClass(lead.situacao)}>
                                        {lead.situacao}
                                    </span>
                                </td>
                                <td>{lead.nome}</td>
                                <td>{formatCurrency(lead.valor_disponivel_para_investimento)}</td>
                                <td>
                                    <span className={getClassificacaoClass(lead.classificacao_index)}>
                                        {lead.classificacao_index}
                                    </span>
                                </td>
                                <td>{lead.origem}</td>
                                <td>{lead.estado}</td>
                                <td>{lead.data_criacao}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {totalPages > 1 && (
                <div className="pagination">
                    <button
                        onClick={() => setCurrentPage(1)}
                        disabled={currentPage === 1}
                    >
                        ««
                    </button>
                    <button
                        onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                        disabled={currentPage === 1}
                    >
                        «
                    </button>
                    <span>
                        Página {currentPage} de {totalPages}
                    </span>
                    <button
                        onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                        disabled={currentPage === totalPages}
                    >
                        »
                    </button>
                    <button
                        onClick={() => setCurrentPage(totalPages)}
                        disabled={currentPage === totalPages}
                    >
                        »»
                    </button>
                </div>
            )}
        </div>
    );
}

export default LeadsTable;
