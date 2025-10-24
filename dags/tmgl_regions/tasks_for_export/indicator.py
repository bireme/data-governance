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

    let year_from = {year_from};
    let filtered;
    if (selectedRegion === "Todas") {
        // Junta dados de todas as regiões
        filtered = Object.values(timeline_year_json).flat();
    } else {
        filtered = timeline_region_year_json[selectedRegion];
    }
    // Selecting all years before starting year
    if (yearFrom === year_from) {
        filtered = filtered.filter((d) => d.ano <= yearTo);
    } else {
        filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
    }

    if (!filtered || filtered.length === 0) {
        total_documents_container.innerText = 0;
        total_fulltext_container.innerText = 0;
        return;
    }

    const total_documents = filtered.reduce((acc, d) => acc + (d.total_documents || 0), 0);
    const total_fulltext = filtered.reduce((acc, d) => acc + (d.total_fulltext || 0), 0);

    total_documents_container.innerText = total_documents.toLocaleString('en-US');
    total_fulltext_container.innerText = total_fulltext.toLocaleString('en-US');
}

const debouncedUpdateIndicators = debounce(updateIndicators, 100);
slider.noUiSlider.on("update", debouncedUpdateIndicators);
regionSelect.addEventListener("change", debouncedUpdateIndicators);
"""


def generate_html_indicators(year_from):
    logger = logging.getLogger(__name__)

    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }