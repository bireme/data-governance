import logging
import json
from airflow.providers.mongo.hooks.mongo import MongoHook


HTML_TEMPLATE = """
function updateIndicators() {
    const year_range = slider.noUiSlider.get(true);
    const yearFrom = parseInt(year_range[0]);
    const yearTo = parseInt(year_range[1]);
    if (yearFrom > yearTo) return;

    const selectedRegion = regionSelect.value;
    const total_documents_container = document.getElementById("indicator_total_documents");
    const total_fulltext_container = document.getElementById("indicator_total_fulltext");

    let filtered;

    if (selectedRegion === "Todas") {
        // Junta dados de todas as regiões
        filtered = Object.values(timeline_year_json)
            .flat()
            .filter((d) => d.ano >= yearFrom && d.ano <= yearTo);

        // Agrupa por ano somando os valores
        const grouped = {};
        filtered.forEach(d => {
            if (!grouped[d.ano]) {
                grouped[d.ano] = {
                    ano: d.ano, 
                    total_documents: 0, 
                    total_fulltext: 0 
                };
            }
            grouped[d.ano].total_documents += d.total_documents || 0;
            grouped[d.ano].total_fulltext += d.total_fulltext || 0;
        });

        // Converte para array
        filtered = Object.values(grouped);
    } else {
        filtered = timeline_region_year_json[selectedRegion]?.filter(
            (d) => d.ano >= yearFrom && d.ano <= yearTo
        ) ?? [];
    }

    if (!filtered || filtered.length === 0) {
        total_documents_container.innerText = 0;
        total_fulltext_container.innerText = 0;
        return;
    }

    const total_documents = filtered.reduce((acc, d) => acc + (d.total_documents || 0), 0);
    const total_fulltext = filtered.reduce((acc, d) => acc + (d.total_fulltext || 0), 0);

    total_documents_container.innerText = total_documents.toLocaleString('pt-BR');
    total_fulltext_container.innerText = total_fulltext.toLocaleString('pt-BR');
}

const debouncedUpdateIndicators = debounce(updateIndicators, 100);
slider.noUiSlider.on("update", debouncedUpdateIndicators);
regionSelect.addEventListener("change", debouncedUpdateIndicators);

updateIndicators();
"""


def generate_html_indicators():
    logger = logging.getLogger(__name__)

    return { 
        'html': HTML_TEMPLATE
    }