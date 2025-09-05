import logging
import json
from airflow.providers.mongo.hooks.mongo import MongoHook


HTML_TEMPLATE = """
const indicator_region_year_json = {indicator_region_year_json};
const indicator_year_json = {indicator_year_json};


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
        filtered = Object.values(indicator_year_json)
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
        filtered = indicator_region_year_json[selectedRegion]?.filter(
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
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Builds indicator_region_year_json
    documents = list(collection.find({"type": "timeline", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        total_documents = doc.get("total", 0)
        total_fulltext = doc.get("with_fulltext", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = {
            "ano": year,
            "total_documents": total_documents,
            "total_fulltext": total_fulltext
        }
        aggregated_data[region].append(year_data)

    indicator_region_year_json = json.dumps(aggregated_data, ensure_ascii=False)


    # Builds indicator_year_json
    documents = list(collection.find({"type": "timeline", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        total_documents = doc.get("total", 0)
        total_fulltext = doc.get("with_fulltext", 0)

        year_data = {
            "ano": year,
            "total_documents": total_documents,
            "total_fulltext": total_fulltext
        }
        aggregated_data.append(year_data)

    indicator_year_json = json.dumps(aggregated_data, ensure_ascii=False)


    html_with_data = HTML_TEMPLATE.replace("{indicator_region_year_json}", indicator_region_year_json)
    html_with_data = html_with_data.replace("{indicator_year_json}", indicator_year_json)

    return { 
        'html': html_with_data
    }