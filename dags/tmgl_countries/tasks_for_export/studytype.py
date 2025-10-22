import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function studytype_loadDataAndRenderChart() {
    const yearResp = await fetch('{country_iso}_studytype_year.json');
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
        colors: [
            "#003b58",
            "#005881",
            "#0074a9",
            "#0093d5",
            "#00aaf5",
            "#6fc0ff",
            "#a7d3ff",
            "#d8eaff",
            "#ecf4ff"
        ],
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

        let year_from = {year_from};
        let filtered;
        filtered = Object.values(studytype_year_json).flat();

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
        sorted.sort((a, b) => b.y - a.y);
        sorted = sorted.slice(0, 20);

        // Atualiza gráfico com dados ordenados
        studytype_chart.series[0].setData(sorted);
    }

    const debouncedUpdateStudytype = debounce(updateStudytypeChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateStudytype);
}

studytype_loadDataAndRenderChart();
"""


def generate_html_studytype(year_from, country, country_iso):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_COUNTRIES_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds studytype_year_json
    documents = list(collection.find({"type": "studytype", "country": country}).sort("year", 1))
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

    output_file = os.path.join(output_dir, f"{country_iso}_studytype_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))
    html_with_data = html_with_data.replace("{country_iso}", country_iso)

    return { 
        'html': html_with_data
    }