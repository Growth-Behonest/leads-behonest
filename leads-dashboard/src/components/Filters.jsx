import React from 'react';
import MultiSelect from './MultiSelect';

function Filters({ filters, setFilters, options }) {
    const handleChange = (field, value) => {
        setFilters(prev => ({ ...prev, [field]: value }));
    };

    const clearFilters = () => {
        setFilters({
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
    };

    const valoresDisponiveis = [
        { value: '0', label: 'R$ 0,00' },
        { value: '20000', label: 'R$ 20.000,00' },
        { value: '30000', label: 'R$ 30.000,00' },
        { value: '55000', label: 'R$ 55.000,00' },
        { value: '60000', label: 'R$ 60.000,00' },
        { value: '100000', label: 'R$ 100.000,00' },
        { value: '120000', label: 'R$ 120.000,00' },
        { value: '150000', label: 'R$ 150.000,00' },
        { value: '151000', label: 'R$ 151.000,00' },
        { value: '200000', label: 'R$ 200.000,00' }
    ];

    return (
        <div className="card fade-in">
            <h2 className="card-title">Filtros</h2>
            <div className="filters-container">
                <MultiSelect
                    label="Etiqueta"
                    options={options.etiquetas}
                    selected={filters.etiqueta}
                    onChange={(val) => handleChange('etiqueta', val)}
                />

                <MultiSelect
                    label="Situação"
                    options={options.situacoes}
                    selected={filters.situacao}
                    onChange={(val) => handleChange('situacao', val)}
                />

                <div className="filter-group">
                    <label>Nome</label>
                    <input
                        type="text"
                        placeholder="Buscar por nome..."
                        value={filters.nome}
                        onChange={(e) => handleChange('nome', e.target.value)}
                    />
                </div>

                <div className="filter-group">
                    <label>Valor Disponível</label>
                    <select
                        value={filters.valor}
                        onChange={(e) => handleChange('valor', e.target.value)}
                    >
                        <option value="">Todos</option>
                        {valoresDisponiveis.map((item, idx) => (
                            <option key={idx} value={item.value}>{item.label}</option>
                        ))}
                    </select>
                </div>

                <MultiSelect
                    label="Classificação Index"
                    options={options.classificacoes}
                    selected={filters.classificacao}
                    onChange={(val) => handleChange('classificacao', val)}
                />

                <MultiSelect
                    label="Origem"
                    options={options.origens}
                    selected={filters.origem}
                    onChange={(val) => handleChange('origem', val)}
                />

                <MultiSelect
                    label="Estado"
                    options={options.estados}
                    selected={filters.estado}
                    onChange={(val) => handleChange('estado', val)}
                />

                <div className="filter-group">
                    <label>Data Início</label>
                    <input
                        type="date"
                        value={filters.dataInicio}
                        onChange={(e) => handleChange('dataInicio', e.target.value)}
                    />
                </div>

                <div className="filter-group">
                    <label>Data Fim</label>
                    <input
                        type="date"
                        value={filters.dataFim}
                        onChange={(e) => handleChange('dataFim', e.target.value)}
                    />
                </div>

                <div className="filter-group" style={{ justifyContent: 'flex-end' }}>
                    <button className="btn-clear" onClick={clearFilters}>
                        Limpar Filtros
                    </button>
                </div>
            </div>
        </div>
    );
}

export default Filters;
