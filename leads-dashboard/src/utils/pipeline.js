
// Valid States
const VALID_STATES = new Set(["MG", "DF", "GO"]);

// City Lists
const FRANCHISE_CITIES_MG = new Set([
    "belo horizonte", "betim", "contagem", "nova lima", "pocos de caldas",
    "pouso alegre", "governador valadares", "ipatinga", "paracatu", "sabara",
    "sarzedo", "ibirite", "igarape", "pedro leopoldo", "vespasiano",
    "ribeirao das neves", "divinopolis", "itabirito", "brumadinho",
    "para de minas", "patos de minas", "esmeraldas", "barbacena", "bom despacho"
]);

const FRANCHISE_CITIES_GO = new Set([
    "anapolis", "aparecida de goiania", "goiania"
]);

const MAX_INVESTMENT = 200000.0;
const SULTS_API_BASE = "https://api.sults.com.br/api/v1/expansao";

/**
 * Normalizes text: remove accents, lowercase.
 */
function normalize(text) {
    if (!text) return "";
    return text.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
}

/**
 * Parses Brazilian currency string to float.
 */
function parseBrCurrency(valueStr) {
    if (!valueStr) return 0.0;
    // Remove non-digit chars except comma and dot
    let clean = valueStr.toString().replace(/[^\d,\.]/g, "");

    if (clean.includes(",") && clean.includes(".")) {
        clean = clean.replace(/\./g, "").replace(",", ".");
    } else if (clean.includes(",")) {
        clean = clean.replace(",", ".");
    } else if (clean.includes(".")) {
        // Ambiguous: 100.000 (100k) vs 100.00 (100.0)
        // Attempt detection: if 3 digits after last dot, assume thousands separator
        const parts = clean.split(".");
        if (parts.length > 1 && parts[parts.length - 1].length === 3) {
            clean = clean.replace(/\./g, "");
        }
    }

    return parseFloat(clean) || 0.0;
}

/**
 * Parses HTML to text using DOMParser.
 */
function parseHtmlToText(html) {
    if (!html) return "";
    const doc = new DOMParser().parseFromString(html, 'text/html');
    return doc.body.textContent || "";
}

/**
 * Extracts investment value from timeline items using Regex.
 */
function parseInvestmentFromTimeline(timelineItems) {
    if (!timelineItems || !Array.isArray(timelineItems)) return 0.0;

    for (const item of timelineItems) {
        const htmlContents = [];
        if (item.descricaoHtml) htmlContents.push(item.descricaoHtml);
        if (item.atividade?.descricaoHtml) htmlContents.push(item.atividade.descricaoHtml);
        if (item.anotacao?.descricaoHtml) htmlContents.push(item.anotacao.descricaoHtml);
        if (item.checkpoint?.descricaoHtml) htmlContents.push(item.checkpoint.descricaoHtml);

        for (const html of htmlContents) {
            const text = parseHtmlToText(html).toLowerCase();

            // Keywords
            if (text.includes("investimento") || text.includes("valor disponivel") || text.includes("capital")) {
                // Regex for numbers: look for patterns like 60.000, 200 mil, etc.
                // Simplified JS logic vs Python regex
                const matches = text.match(/([\d\.,]+)/g);
                if (matches) {
                    for (const valStr of matches) {
                        let val = parseBrCurrency(valStr);

                        // Check context for "mil"
                        const idx = text.indexOf(valStr);
                        const suffix = text.substring(idx + valStr.length, idx + valStr.length + 20);
                        if (suffix.includes("mil") || suffix.includes("mi ")) {
                            if (val < 1000) val *= 1000;
                        }

                        if (val >= 1000) return val;
                    }
                }
            }
        }
    }
    return 0.0;
}

/**
 * Calculates Location Index.
 */
function calculateLocationIndex(lead) {
    const estado = (lead.estado || "").toUpperCase().trim();
    const cidade = normalize(lead.cidade || "");

    if (!VALID_STATES.has(estado)) return 0;
    if (estado === "DF") return 1;

    if (estado === "GO") {
        if (FRANCHISE_CITIES_GO.has(cidade)) return 1;
        for (const fc of FRANCHISE_CITIES_GO) {
            if (cidade.includes(fc) && fc.length >= 5) return 1;
        }
    }

    if (estado === "MG") {
        if (FRANCHISE_CITIES_MG.has(cidade)) return 1;
        for (const fc of FRANCHISE_CITIES_MG) {
            if (cidade.includes(fc) && fc.length >= 5) return 1;
        }
    }

    return 0;
}

/**
 * Calculates Investment Index.
 */
function calculateInvestmentIndex(value) {
    if (value <= 0) return 0.0;
    return Math.min(value / MAX_INVESTMENT, 1.0);
}

/**
 * Calculates Time Index based on dataset min/max dates.
 */
function calculateTimeIndex(dateStr, minDate, maxDate) {
    if (!dateStr) return 0.0;
    // Parse DD/MM/YYYY
    const parts = dateStr.split("/");
    if (parts.length !== 3) return 0.0;
    const d = new Date(`${parts[2]}-${parts[1]}-${parts[0]}`);

    const minTime = minDate.getTime();
    const maxTime = maxDate.getTime();
    const currTime = d.getTime();

    if (maxTime === minTime) return 1.0;

    // (max - current) / (max - min)
    // Newest leads (max date) -> 0 index?
    // Wait, python script: min_days (recent) vs max_days (old).
    // Formula: (max_days - days_ago) / (max_days - min_days).
    // If days_ago = min_days (most recent), num = max - min -> result = 1.
    // If days_ago = max_days (oldest), num = 0 -> result = 0.
    // So NEWEST leads get 1.0. 

    // In timestamp: maxTime is NEWEST. minTime is OLDEST.
    // index = (current - min) / (max - min) -> Newest (current=max) gets 1.

    const index = (currTime - minTime) / (maxTime - minTime);
    return Math.max(0, Math.min(index, 1.0));
}

/**
 * Main Pipeline Function
 * @param {string} token - SULTS API Token
 * @param {function} onProgress - Callback for progress updates
 */
export async function runClientPipeline(token, onProgress) {
    if (!onProgress) onProgress = () => { };

    const headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=UTF-8"
    };

    // 1. Fetch All Leads
    onProgress("Baixando leads (Página 1...)...");
    let allLeads = [];
    let page = 0;
    const LIMIT = 100;

    while (true) {
        onProgress(`Baixando página ${page + 1}...`);
        try {
            const resp = await fetch(`${SULTS_API_BASE}/negocio?start=${page}&limit=${LIMIT}`, { headers });
            if (!resp.ok) throw new Error(`Erro na API: ${resp.status}`);
            const data = await resp.json();

            const leadsPage = data.data || [];
            if (leadsPage.length === 0) break;

            // Filter Funnel ID 1
            const filtered = leadsPage.filter(l => l.etapa?.funil?.id === 1);
            allLeads.push(...filtered);

            if ((page + 1) >= data.totalPage) break;
            page++;

            // Safety break
            if (page > 50) break;
        } catch (err) {
            console.error(err);
            onProgress(`Erro ao baixar página ${page}: ${err.message}`);
            break;
        }
    }

    onProgress(`Total de leads brutos: ${allLeads.length}. Iniciando enriquecimento...`);

    // 2. Enrich Leads (Fetch Timeline)
    // Limit concurrency manually
    const enrichedLeads = [];
    const BATCH_SIZE = 5;

    for (let i = 0; i < allLeads.length; i += BATCH_SIZE) {
        const batch = allLeads.slice(i, i + BATCH_SIZE);
        const promises = batch.map(async (lead) => {
            // Filters
            if ([7286, 4918, 2067, 2090].includes(lead.id)) return null;
            const search = `${lead.nome || ''} ${lead.email || ''} ${lead.titulo || ''}`.toLowerCase();
            if (search.includes("teste")) return null;
            if (lead.origem?.nome?.toUpperCase().includes("DUPLICADO")) return null;

            // Fetch Timeline
            let availableValue = 0.0;
            try {
                const tResp = await fetch(`${SULTS_API_BASE}/negocio/${lead.id}/timeline`, { headers });
                if (tResp.ok) {
                    const tData = await tResp.json();
                    availableValue = parseInvestmentFromTimeline(tData.data);
                }
            } catch (e) {
                console.warn(`Failed timeline for ${lead.id}`, e);
            }

            // Normalize fields
            const dateStr = formatDatetBr(lead.dtCadastro); // Function to impl

            // Return structured object
            return {
                id: lead.id,
                data_criacao: dateStr,
                titulo: lead.titulo,
                nome: lead.contatoPessoa?.[0]?.nome,
                email: lead.contatoPessoa?.[0]?.email,
                celular: lead.contatoPessoa?.[0]?.phone,
                origem: lead.origem?.nome,
                cidade: lead.cidade,
                estado: lead.uf, // Will fallback logic later
                etiquetas: (lead.etiqueta || []).map(t => t.nome).join(", "),
                situacao: lead.situacao?.nome,
                motivo_perda: lead.situacaoPerdaMotivo?.nome,
                valor_disponivel_para_investimento: availableValue,
                etapa_funil: lead.etapa?.nome
            };
        });

        const results = await Promise.all(promises);
        enrichedLeads.push(...results.filter(r => r !== null));
        onProgress(`Processado ${Math.min(i + BATCH_SIZE, allLeads.length)} / ${allLeads.length} leads...`);
    }

    // 3. Process Indices
    onProgress("Calculando índices e scores...");

    // Find Date Range for Time Index
    let minDate = new Date();
    let maxDate = new Date(0);

    enrichedLeads.forEach(l => {
        const parts = l.data_criacao.split("/");
        if (parts.length === 3) {
            const d = new Date(`${parts[2]}-${parts[1]}-${parts[0]}`);
            if (d < minDate) minDate = d;
            if (d > maxDate) maxDate = d;
        }
    });

    const finalLeads = enrichedLeads.map(lead => {
        // 1. Location Index
        // Logic for inferring state if missing is omitted for brevity but can be added
        const locIndex = calculateLocationIndex(lead);
        lead.localizacao_index = locIndex; // logic

        // 2. Investment Index
        const invIndex = calculateInvestmentIndex(lead.valor_disponivel_para_investimento);
        lead.investimento_index = invIndex.toFixed(2).replace(".", ",");

        // 3. Time Index
        const timeIdx = calculateTimeIndex(lead.data_criacao, minDate, maxDate);
        lead.tempo_index = timeIdx.toFixed(2).replace(".", ",");

        // 4. Score
        // (loc*3) + (inv*2) + (time*0.5)
        const score = (locIndex * 3) + (invIndex * 2) + (timeIdx * 0.5);
        lead.score_index = score.toFixed(2).replace(".", ",");

        // 5. Classification
        if (score >= 4.09) lead.classificacao_index = "MQL+";
        else if (score >= 3.58) lead.classificacao_index = "MQL";
        else if (score >= 3.00) lead.classificacao_index = "LEAD+";
        else if (score >= 0.62) lead.classificacao_index = "LEAD";
        else lead.classificacao_index = "DESQUALIFICADO 100%";

        // Format Value
        lead.valor_disponivel_para_investimento = lead.valor_disponivel_para_investimento.toLocaleString('pt-BR', { minimumFractionDigits: 2 });

        return lead;
    });

    return finalLeads;
}

function formatDatetBr(iso_str) {
    if (!iso_str) return "";
    const datePart = iso_str.split("T")[0]; // YYYY-MM-DD
    const parts = datePart.split("-");
    return `${parts[2]}/${parts[1]}/${parts[0]}`;
}
