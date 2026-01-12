import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

const COLORS = [
    '#f9a01b', // Laranja
    '#dc3545', // Vermelho
    '#17a2b8', // Ciano
    '#6f42c1', // Roxo
    '#20c997', // Verde claro
    '#fd7e14', // Laranja claro
    '#0d6efd', // Azul
    '#198754', // Verde
    '#6610f2', // Indigo
    '#ffc107', // Amarelo
];

function ClassificationLossChart({ data }) {
    // Filtra apenas leads com perda (situacao = PERDA e motivo_perda preenchido)
    const lostLeads = data.filter(lead =>
        lead.situacao === 'PERDA' && lead.motivo_perda && lead.motivo_perda.trim() !== ''
    );

    if (lostLeads.length === 0) {
        return (
            <div className="card fade-in">
                <h2 className="card-title">Classificação × Motivo de Perda</h2>
                <div className="no-results">
                    <p>Sem dados de perda para exibir</p>
                </div>
            </div>
        );
    }

    // Agrupa por classificação e motivo de perda
    const grouped = {};
    const allMotivos = new Set();

    lostLeads.forEach(lead => {
        const classificacao = lead.classificacao_index || 'N/A';
        const motivo = lead.motivo_perda.trim();

        allMotivos.add(motivo);

        if (!grouped[classificacao]) {
            grouped[classificacao] = {};
        }
        if (!grouped[classificacao][motivo]) {
            grouped[classificacao][motivo] = 0;
        }
        grouped[classificacao][motivo]++;
    });

    // Converte para formato do Recharts
    const chartData = Object.entries(grouped).map(([classificacao, motivos]) => {
        const entry = { classificacao };
        Object.entries(motivos).forEach(([motivo, count]) => {
            entry[motivo] = count;
        });
        return entry;
    });

    // Ordena por total de perdas
    chartData.sort((a, b) => {
        const totalA = Object.values(a).filter(v => typeof v === 'number').reduce((sum, v) => sum + v, 0);
        const totalB = Object.values(b).filter(v => typeof v === 'number').reduce((sum, v) => sum + v, 0);
        return totalB - totalA;
    });

    // Pega os top 6 motivos mais frequentes
    const motivoCounts = {};
    lostLeads.forEach(lead => {
        const motivo = lead.motivo_perda.trim();
        motivoCounts[motivo] = (motivoCounts[motivo] || 0) + 1;
    });

    const topMotivos = Object.entries(motivoCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 6)
        .map(([motivo]) => motivo);

    const CustomTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            return (
                <div style={{
                    background: 'rgba(2, 27, 78, 0.95)',
                    padding: '12px 16px',
                    borderRadius: '8px',
                    border: '1px solid rgba(255, 255, 255, 0.2)',
                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
                    maxWidth: '250px'
                }}>
                    <p style={{ color: '#f9a01b', fontWeight: 600, marginBottom: '8px' }}>
                        {label}
                    </p>
                    {payload.map((entry, index) => (
                        <p key={index} style={{ color: entry.color, fontSize: '0.85rem', margin: '4px 0' }}>
                            {entry.name}: {entry.value}
                        </p>
                    ))}
                </div>
            );
        }
        return null;
    };

    return (
        <div className="card fade-in" style={{ gridColumn: 'span 2' }}>
            <h2 className="card-title">Classificação × Motivo de Perda</h2>
            <p style={{ color: '#aaa', fontSize: '0.85rem', marginBottom: '1rem' }}>
                Análise de {lostLeads.length} leads perdidos
            </p>
            <div className="chart-container chart-responsive-wrapper">
                <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={chartData} layout="vertical" margin={{ left: 20, right: 20 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                        <XAxis type="number" stroke="#fff" tick={{ fill: '#ccc', fontSize: 12 }} />
                        <YAxis
                            type="category"
                            dataKey="classificacao"
                            stroke="#fff"
                            tick={{ fill: '#ccc', fontSize: 12 }}
                            width={120}
                        />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend
                            wrapperStyle={{ paddingTop: '20px' }}
                            formatter={(value) => <span style={{ color: '#ccc', fontSize: '0.8rem' }}>{value}</span>}
                        />
                        {topMotivos.map((motivo, index) => (
                            <Bar
                                key={motivo}
                                dataKey={motivo}
                                stackId="a"
                                fill={COLORS[index % COLORS.length]}
                                name={motivo}
                            />
                        ))}
                    </BarChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}

export default ClassificationLossChart;
