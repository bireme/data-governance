import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function subject_loadDataAndRenderChart() {
    const regionResp = await fetch('subject_region_year.json');
    const subject_region_year_json = await regionResp.json();

    const yearResp = await fetch('subject_year.json');
    const subject_year_json = await yearResp.json();

    Highcharts.setOptions({
        lang: {
            thousandsSep: ' '
        }
    });

    let subject_chart = Highcharts.chart("subject_container", {
        chart: { 
            type: 'wordcloud',
            backgroundColor: '#F7F7F8',
            borderRadius: 16,
            borderColor: '#C7C6C0',
            borderWidth: 2,
            spacingTop: 20,
            height: 325
        },
        colors: [
            "#003b58",
            "#005881",
            "#0074a9",
            "#0093d5",
            "#00aaf5",
            "#6fc0ff",
            "#a7d3ff"
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
            wordcloud: {
                rotation: {
                    from: -30,
                    to: 30,
                    orientations: 5
                },
                minFontSize: 16,
                maxFontSize: 50,
                dataLabels: {
                    enabled: true
                }
            }
        },
        series: [{ 
            type: 'wordcloud',
            name: "Number of documents", 
            data: [], 
            colorByPoint: true
        }],
    });

    function updateSubjectChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(subject_year_json).flat();
        } else {
            filtered = subject_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            subject_chart.series[0].setData([]);
            subject_chart.showNoData();
            return;
        } else {
            subject_chart.hideNoData();
        }

        const subjects = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            subjects.forEach((subject) => {
                total[subject] = (total[subject] || 0) + (d[subject] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = subjects.map((subject) => ({
            name: subject,
            weight: total[subject]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.weight - a.weight);
        sorted = sorted.slice(0, 100);

        // Atualiza gráfico com dados ordenados
        subject_chart.series[0].setData(sorted);
    }

    const debouncedUpdateSubject = debounce(updateSubjectChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateSubject);
    regionSelect.addEventListener("change", debouncedUpdateSubject);
}

subject_loadDataAndRenderChart();
"""


def generate_html_subject(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_REGION_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds subject_region_year_json
    documents = list(collection.find({"type": "subject", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        subject = doc["name"]
        count = doc.get("count", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data[region].append(year_data)

        year_data[subject] = count

    output_file = os.path.join(output_dir, "subject_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds subject_year_json
    documents = list(collection.find({"type": "subject", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        subject = doc["name"]
        count = doc.get("count", 0)

        year_data = next((item for item in aggregated_data if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data.append(year_data)

        year_data[subject] = count

    output_file = os.path.join(output_dir, "subject_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }