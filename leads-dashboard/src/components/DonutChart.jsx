import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';

const COLORS = [
    '#f9a01b', // Laranja - MQL+
    '#ffcc00', // Amarelo - MQL
    '#17a2b8', // Ciano - LEAD+
    '#20c997', // Verde claro - LEAD
    '#dc3545', // Vermelho - DESQUALIFICADO
    '#6f42c1', // Roxo
    '#fd7e14', // Laranja claro
    '#198754', // Verde
];

function DonutChart({ data }) {
    // Agrupa dados por classificacao_index
    const groupedData = data.reduce((acc, lead) => {
        const key = lead.classificacao_index || 'N/A';
        if (!acc[key]) {
            acc[key] = 0;
        }
        acc[key]++;
        return acc;
    }, {});

    const chartData = Object.entries(groupedData)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value);

    const total = data.length;

    const CustomTooltip = ({ active, payload }) => {
        if (active && payload && payload.length) {
            const item = payload[0];
            const percentage = ((item.value / total) * 100).toFixed(1);
            return (
                <div style={{
                    background: 'rgba(2, 27, 78, 0.95)',
                    padding: '12px 16px',
                    borderRadius: '8px',
                    border: '1px solid rgba(255, 255, 255, 0.2)',
                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)'
                }}>
                    <p style={{ color: item.payload.fill, fontWeight: 600, marginBottom: '4px' }}>
                        {item.name}
                    </p>
                    <p style={{ color: '#fff', fontSize: '0.9rem' }}>
                        {item.value} leads ({percentage}%)
                    </p>
                </div>
            );
        }
        return null;
    };

    if (chartData.length === 0) {
        return (
            <div className="card fade-in">
                <h2 className="card-title">Distribuição por Classificação</h2>
                <div className="no-results">
                    <p>Sem dados para exibir</p>
                </div>
            </div>
        );
    }

    return (
        <div className="card fade-in">
            <h2 className="card-title">Distribuição por Classificação</h2>
            <div className="chart-container">
                <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                        <Pie
                            data={chartData}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            paddingAngle={2}
                            dataKey="value"
                        >
                            {chartData.map((entry, index) => (
                                <Cell
                                    key={`cell-${index}`}
                                    fill={COLORS[index % COLORS.length]}
                                    stroke="rgba(255, 255, 255, 0.1)"
                                    strokeWidth={2}
                                />
                            ))}
                        </Pie>
                        <Tooltip content={<CustomTooltip />} />
                    </PieChart>
                </ResponsiveContainer>

                <div className="chart-legend">
                    {chartData.map((item, index) => (
                        <div key={item.name} className="legend-item">
                            <span
                                className="legend-color"
                                style={{ backgroundColor: COLORS[index % COLORS.length] }}
                            />
                            <span>{item.name}: {item.value}</span>
                        </div>
                    ))}
                </div>

                <div style={{
                    textAlign: 'center',
                    marginTop: '1rem',
                    padding: '1rem',
                    background: 'rgba(249, 160, 27, 0.1)',
                    borderRadius: '8px'
                }}>
                    <div style={{ fontSize: '2rem', fontWeight: 700, color: '#f9a01b' }}>
                        {total}
                    </div>
                    <div style={{ color: '#ccc', fontSize: '0.9rem' }}>
                        Total de Leads
                    </div>
                </div>
            </div>
        </div>
    );
}

export default DonutChart;
