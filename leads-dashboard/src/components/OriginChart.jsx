import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';

const COLORS = [
    '#f9a01b', // Laranja
    '#ffcc00', // Amarelo
    '#17a2b8', // Ciano
    '#20c997', // Verde claro
    '#dc3545', // Vermelho
    '#6f42c1', // Roxo
    '#fd7e14', // Laranja claro
    '#198754', // Verde
    '#0d6efd', // Azul
    '#6610f2', // Indigo
];

function OriginChart({ data }) {
    // Agrupa dados por origem
    const groupedData = data.reduce((acc, lead) => {
        const key = lead.origem || 'N/A';
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
                <h2 className="card-title">Distribuição por Origem</h2>
                <div className="no-results">
                    <p>Sem dados para exibir</p>
                </div>
            </div>
        );
    }

    // Limita a exibição da legenda para os top 5, o resto fica no tooltip
    const topOrigins = chartData.slice(0, 8);

    return (
        <div className="card fade-in">
            <h2 className="card-title">Distribuição por Origem</h2>
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
                    {topOrigins.map((item, index) => (
                        <div key={item.name} className="legend-item">
                            <span
                                className="legend-color"
                                style={{ backgroundColor: COLORS[index % COLORS.length] }}
                            />
                            <span>{item.name}: {item.value}</span>
                        </div>
                    ))}
                    {chartData.length > 8 && (
                        <div className="legend-item">
                            <span style={{ color: '#ccc', fontStyle: 'italic' }}>
                                + {chartData.length - 8} outras...
                            </span>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

export default OriginChart;
