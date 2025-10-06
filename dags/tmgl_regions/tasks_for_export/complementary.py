import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function complementary_loadDataAndRenderChart() {
    const regionResp = await fetch('complementary_region_year.json');
    const complementary_region_year_json = await regionResp.json();

    const yearResp = await fetch('complementary_year.json');
    const complementary_year_json = await yearResp.json();

    let complementary_chart = Highcharts.chart("complementary_container", {
        chart: { 
            type: "bar",
            backgroundColor: '#F7F7F8',
            borderRadius: 16,
            borderColor: '#C7C6C0',
            borderWidth: 2,
            spacingTop: 20,
            height: 325
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
        lang: {
            noData: 'No data to display for this filter combination'
        },
        noData: {
            style: {
                fontSize: '15px'
            }
        },
        legend: { enabled: false },
        xAxis: { 
            title: { text: null },
            labels: {
                rotation: 0,
                step: 1,
                style: {
                    fontSize: '14px'
                }
            }
        },
        yAxis: {
            min: 0,
            title: { text: "Number of documents" }
        },
        plotOptions: {
            bar: {
                dataLabels: { enabled: true },
            },
        },
        series: [{ name: "Number of documents", data: [], color: "#0093d5" }],
    });

    function updateComplementaryChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(complementary_year_json).flat();
        } else {
            filtered = complementary_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            complementary_chart.series[0].setData([]);
            complementary_chart.update({ xAxis: { categories: [] } });
            complementary_chart.showNoData();
            return;
        } else {
            complementary_chart.hideNoData();
        }

        const complementaries = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            complementaries.forEach((complementary) => {
                total[complementary] = (total[complementary] || 0) + (d[complementary] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = complementaries.map((complementary) => ({
            name: complementary,
            value: total[complementary]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.value - a.value);
        sorted = sorted.slice(0, 40);

        // Atualiza gráfico com dados ordenados
        complementary_chart.series[0].setData(sorted.map(item => item.value));
        complementary_chart.update({ xAxis: { categories: sorted.map(item => item.name) } });
    }

    const debouncedUpdateComplementary = debounce(updateComplementaryChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateComplementary);
    regionSelect.addEventListener("change", debouncedUpdateComplementary);
}

complementary_loadDataAndRenderChart();
"""


def generate_html_complementary(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds complementary_region_year_json
    documents = list(collection.find({"type": "complementary", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        name = doc["name"].replace("_", " ")
        count = doc.get("count", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data[region].append(year_data)

        year_data[name] = count

    output_file = os.path.join(output_dir, "complementary_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds complementary_year_json
    documents = list(collection.find({"type": "complementary", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        name = doc["name"].replace("_", " ")
        count = doc.get("count", 0)

        year_data = next((item for item in aggregated_data if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data.append(year_data)

        year_data[name] = count

    output_file = os.path.join(output_dir, "complementary_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }