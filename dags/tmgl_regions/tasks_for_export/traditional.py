import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function traditional_loadDataAndRenderChart() {
    const regionResp = await fetch('traditional_region_year.json');
    const traditional_region_year_json = await regionResp.json();

    const yearResp = await fetch('traditional_year.json');
    const traditional_year_json = await yearResp.json();

    let traditional_chart = Highcharts.chart("traditional_container", {
        chart: { 
            type: 'lollipop',
            inverted: true,
            backgroundColor: '#F7F7F8',
            borderRadius: 16,
            borderColor: '#C7C6C0',
            borderWidth: 2,
            spacingTop: 20,
            height: 700
        },
        exporting: {
            buttons: {
                contextButton: {
                    theme: {
                        fill: '#F7F7F8'
                    }
                }
            }
        },
        title: { 
            text: ""
        },
        legend: {
            enabled: false
        },
        xAxis: {
            type: 'category'
        },
        yAxis: {
            min: 1,
            title: { text: "Number of documents" },
            type: "logarithmic"
        },
        lang: {
            noData: 'No data to display for this filter combination'
        },
        noData: {
            style: {
                fontSize: '15px'
            }
        },
        series: [{ 
            name: "Number of documents", 
            data: [],
            color: "#0093d5"
        }],
    });

    function updateTraditionalChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(traditional_year_json).flat();
        } else {
            filtered = traditional_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            traditional_chart.series[0].setData([]);
            traditional_chart.showNoData();
            return;
        } else {
            traditional_chart.hideNoData();
        }

        const data = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            data.forEach((traditional) => {
                total[traditional] = (total[traditional] || 0) + (d[traditional] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = data.map((traditional) => ({
            name: traditional,
            y: total[traditional]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.y - a.y);
        sorted = sorted.slice(0, 30);

        // Atualiza gráfico com dados ordenados
        traditional_chart.series[0].setData(sorted);
    }

    const debouncedUpdateTraditional = debounce(updateTraditionalChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateTraditional);
    regionSelect.addEventListener("change", debouncedUpdateTraditional);
}

traditional_loadDataAndRenderChart();
"""


def generate_html_traditional(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds traditional_region_year_json
    documents = list(collection.find({"type": "traditional", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        name = doc["name"]
        count = doc.get("count", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data[region].append(year_data)

        year_data[name] = count

    output_file = os.path.join(output_dir, "traditional_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds traditional_year_json
    documents = list(collection.find({"type": "traditional", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        name = doc["name"]
        count = doc.get("count", 0)

        year_data = next((item for item in aggregated_data if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data.append(year_data)

        year_data[name] = count

    output_file = os.path.join(output_dir, "traditional_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }