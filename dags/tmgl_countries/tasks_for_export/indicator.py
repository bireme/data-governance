import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function indicator_loadDataAndRenderChart() {

    const yearResp = await fetch('{country_iso}_indicator_year.json');
    const indicator_year_json = await yearResp.json();

    function updateIndicators() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const total_documents_container = document.getElementById("indicator_total_documents");
        const total_fulltext_container = document.getElementById("indicator_total_fulltext");

        let year_from = {year_from};
        let filtered;
        filtered = Object.values(indicator_year_json).flat();

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
}

indicator_loadDataAndRenderChart();
"""


def generate_html_indicators(year_from, country, country_iso):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_COUNTRIES_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    documents = list(collection.find({"type": "timeline", "country": country}).sort("year", 1))
    aggregated_data = []
    years = []
    for doc in documents:
        year = int(doc["year"])
        total_documents = doc.get("total", 0)
        total_fulltext = doc.get("with_fulltext", 0)
        years.append(year)

        year_data = {
            "ano": year,
            "total_documents": total_documents,
            "total_fulltext": total_fulltext
        }
        aggregated_data.append(year_data)

    output_file = os.path.join(output_dir, f"{country_iso}_indicator_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)

    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))
    html_with_data = html_with_data.replace("{country_iso}", country_iso)
    min_year = min(years)
    max_year = max(years)

    return { 
        'html': html_with_data,
        'min_year': min_year,
        'max_year': max_year,
    }