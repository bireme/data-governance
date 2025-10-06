import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function therapy_loadDataAndRenderChart() {
    const therapyResp = await fetch('therapy_region_year.json');
    const therapy_region_year_json = await therapyResp.json();

    const yearResp = await fetch('therapy_year.json');
    const therapy_year_json = await yearResp.json();

    let therapy_chart = Highcharts.chart("therapy_container", {
        chart: { 
            type: 'pie',
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
                borderRadius: 5,
                dataLabels: [{
                    enabled: true,
                    distance: 15,
                    format: '{point.name}'
                }, {
                    enabled: true,
                    distance: '-30%',
                    filter: {
                        property: 'percentage',
                        operator: '>',
                        value: 5
                    },
                    format: '{point.percentage:.1f}%',
                    style: {
                        fontSize: '0.9em',
                        textOutline: 'none'
                    }
                }]
            }
        },
        tooltip: {
            headerFormat: '',
            pointFormat: '<b>{point.name}</b><br>' +
                'Number of documents: <b>{point.y}</b><br/>' +
                '<b>{point.percentage:.2f}%</b> of total<br/>'
        },
        series: [{ 
            name: "Therapeutic Methods and Therapies", 
            data: [], 
        }],
        drilldown: {
            series: []
        }
    });

    function updateTherapyChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(therapy_year_json).flat();
        } else {
            filtered = therapy_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            therapy_chart.series[0].setData([]);
            therapy_chart.showNoData();
            return;
        } else {
            therapy_chart.hideNoData();
        }

        const therapies = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            therapies.forEach((therapy) => {
                total[therapy] = (total[therapy] || 0) + (d[therapy] || 0);
            });
        });

        // Monta pares terapia/valor
        let sorted = therapies.map((therapy) => ({
            name: therapy,
            y: total[therapy]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.y - a.y);

        const therapyDataMap = {};
        const drilldownMap = {};

        sorted.forEach(({name, y}) => {
            const [level1, level2] = name.split('/');
            if (!therapyDataMap[level1]) {
                therapyDataMap[level1] = 0;
                drilldownMap[level1] = {
                    name: level1,
                    id: level1,
                    data: []
                };
            }
            therapyDataMap[level1] += y;
            drilldownMap[level1].data.push([level2, y]);
        });

        // Criar o array data com soma dos valores por região
        const data = Object.entries(therapyDataMap).map(([level1, sum]) => ({
            name: level1.toUpperCase(),
            y: sum,
            drilldown: level1
        }));

        const drilldown = {
            series: Object.values(drilldownMap)
        };

        // Atualiza gráfico com dados ordenados
        therapy_chart.update({
            drilldown: {
                series: drilldown.series
            }
        }, false);

        therapy_chart.series[0].setData(data);
    }

    const debouncedUpdateTherapy = debounce(updateTherapyChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateTherapy);
    regionSelect.addEventListener("change", debouncedUpdateTherapy);
}

therapy_loadDataAndRenderChart();
"""


def generate_html_therapy(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds therapy_region_year_json
    documents = list(collection.find({"type": "therapy", "region": {"$ne": None}}).sort("year", 1))
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

    output_file = os.path.join(output_dir, "therapy_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds region_year_json
    documents = list(collection.find({"type": "therapy", "region": None}).sort("year", 1))
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

    output_file = os.path.join(output_dir, "therapy_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }