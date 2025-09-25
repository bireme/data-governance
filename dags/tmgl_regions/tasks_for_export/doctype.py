import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function doctype_loadDataAndRenderChart() {
    const regionResp = await fetch('doctype_region_year.json');
    const doctype_region_year_json = await regionResp.json();

    const yearResp = await fetch('doctype_year.json');
    const doctype_year_json = await yearResp.json();

    let doctype_chart = Highcharts.chart("doctype_container", {
        chart: { 
            type: 'column',
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
        legend: { enabled: false },
        lang: {
            noData: 'No data to display for this filter combination'
        },
        noData: {
            style: {
                fontSize: '15px'
            }
        },
        yAxis: {
            min: 1,
            title: { text: "Number of documents" },
            type: "logarithmic"
        },
        plotOptions: {
            column: {
                dataLabels: { enabled: true },
            },
        },
        series: [{ 
            name: "Number of documents",
            data: [], 
            color: "#0093d5"
        }],
    });

    function updateDoctypeChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(doctype_year_json).flat();
        } else {
            filtered = doctype_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            doctype_chart.series[0].setData([]);
            doctype_chart.showNoData();
            return;
        } else {
            doctype_chart.hideNoData();
        }

        const doctypes = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            doctypes.forEach((doctype) => {
                total[doctype] = (total[doctype] || 0) + (d[doctype] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = doctypes.map((doctype) => ({
            name: doctype,
            value: total[doctype]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.value - a.value);
        sorted = sorted.slice(0, 20);

        // Atualiza gráfico com dados ordenados
        doctype_chart.series[0].setData(sorted.map(item => item.value));
        doctype_chart.update({ xAxis: { categories: sorted.map(item => item.name) } });
    }

    const debouncedUpdateDoctype = debounce(updateDoctypeChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateDoctype);
    regionSelect.addEventListener("change", debouncedUpdateDoctype);
}

doctype_loadDataAndRenderChart();
"""


def generate_html_doctype(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds doctype_region_year_json
    documents = list(collection.find({"type": "doctype", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        doctype = doc["name"]
        count = doc.get("count", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data[region].append(year_data)

        year_data[doctype] = count

    output_file = os.path.join(output_dir, "doctype_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds doctype_year_json
    documents = list(collection.find({"type": "doctype", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        doctype = doc["name"]
        count = doc.get("count", 0)

        year_data = next((item for item in aggregated_data if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data.append(year_data)

        year_data[doctype] = count

    output_file = os.path.join(output_dir, "doctype_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }