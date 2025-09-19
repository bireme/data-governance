import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function studytype_loadDataAndRenderChart() {
    const regionResp = await fetch('studytype_region_year.json');
    const studytype_region_year_json = await regionResp.json();

    const yearResp = await fetch('studytype_year.json');
    const studytype_year_json = await yearResp.json();

    let studytype_chart = Highcharts.chart("studytype_container", {
        chart: { 
            type: 'pie',
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
        lang: {
            noData: 'No data to display for this filter combination'
        },
        noData: {
            style: {
                fontSize: '15px'
            }
        },
        plotOptions: {
        pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    format: `<b>{point.name}</b>: {point.percentage:.2f}%`
                }
            }
        },
        series: [{ 
            name: "Number of documents", 
            data: [], 
            colorByPoint: true
        }],
    });

    function updateStudytypeChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(studytype_year_json).flat();
        } else {
            filtered = studytype_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            studytype_chart.series[0].setData([]);
            studytype_chart.showNoData();
            return;
        } else {
            studytype_chart.hideNoData();
        }

        const studytypes = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            studytypes.forEach((studytype) => {
                total[studytype] = (total[studytype] || 0) + (d[studytype] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = studytypes.map((studytype) => ({
            name: studytype,
            y: total[studytype]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.value - a.value);
        sorted = sorted.slice(0, 20);

        // Atualiza gráfico com dados ordenados
        studytype_chart.series[0].setData(sorted);
    }

    const debouncedUpdateStudytype = debounce(updateStudytypeChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateStudytype);
    regionSelect.addEventListener("change", debouncedUpdateStudytype);

    updateStudytypeChart();
}

studytype_loadDataAndRenderChart();
"""


def generate_html_studytype(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds studytype_region_year_json
    documents = list(collection.find({"type": "studytype", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        studytype = doc["name"]
        count = doc.get("count", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data[region].append(year_data)

        year_data[studytype] = count

    output_file = os.path.join(output_dir, "studytype_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds studytype_year_json
    documents = list(collection.find({"type": "studytype", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        studytype = doc["name"]
        count = doc.get("count", 0)

        year_data = next((item for item in aggregated_data if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data.append(year_data)

        year_data[studytype] = count

    output_file = os.path.join(output_dir, "studytype_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }