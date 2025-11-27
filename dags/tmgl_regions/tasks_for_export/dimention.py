import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function dimention_loadDataAndRenderChart() {
    const dimentionResp = await fetch('dimention_region_year.json');
    const dimention_region_year_json = await dimentionResp.json();

    const yearResp = await fetch('dimention_year.json');
    const dimention_year_json = await yearResp.json();

    Highcharts.setOptions({
        lang: {
            thousandsSep: ' '
        }
    });

    let dimention_chart = Highcharts.chart("dimention_container", {
        chart: { 
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
        tooltip: {
            pointFormat: '<b>{point.name}</b><br>' +
                'Number of documents: <b>{point.value}</b>'
        },
        series: [{
            type: 'treemap',
            name: 'Traditional Medicine Dimension',
            allowTraversingTree: true,
            alternateStartingDirection: true,
            dataLabels: {
                format: '{point.name}',
                style: {
                    textOutline: 'none'
                }
            },
            borderRadius: 3,
            nodeSizeBy: 'leaf',
            levels: [
                {
                    level: 1,
                    layoutAlgorithm: 'sliceAndDice',
                    groupPadding: 3,
                    dataLabels: {
                        headers: true,
                        enabled: true,
                        style: {
                            fontSize: '0.6em',
                            fontWeight: 'normal',
                            textTransform: 'uppercase',
                            color: 'var(--highcharts-neutral-color-100, #000)'
                        }
                    },
                    borderRadius: 3,
                    borderWidth: 1,
                    colorByPoint: true
                }, 
                {
                    level: 2,
                    dataLabels: {
                        enabled: true,
                        inside: false
                    }
                }
            ],
            data: []
        }]
    });

    function updateDimentionChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(dimention_year_json).flat();
        } else {
            filtered = dimention_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            dimention_chart.series[0].setData([]);
            dimention_chart.showNoData();
            return;
        } else {
            dimention_chart.hideNoData();
        }

        const dimentions = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            dimentions.forEach((dimention) => {
                total[dimention] = (total[dimention] || 0) + (d[dimention] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = dimentions.map((dimention) => ({
            name: dimention,
            y: total[dimention]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.y - a.y);

        // Cria um mapa para identificar os pais e gerar ids
        const parentMap = {};
        let idCounter = 1;

        // Gera uma id para cada pai único
        sorted.forEach(item => {
            const [parent] = item.name.split('/');
            if (!parentMap[parent]) {
                parentMap[parent] = String.fromCharCode(64 + idCounter); // 'A', 'B', 'C' etc.
                idCounter++;
            }
        });

        // Monta o array final
        const data = [];

        // Adiciona os nós pais com id e cor vazia (você pode definir cores se quiser)
        for (const parent in parentMap) {
            data.push({
                id: parentMap[parent],
                name: parent
            });
        }

        // Adiciona os filhos referenciando o parent pelo id
        sorted.forEach(item => {
            const [parent, child] = item.name.split('/');
            data.push({
                name: child,
                parent: parentMap[parent],
                value: item.y
            });
        });

        dimention_chart.series[0].setData(data);
    }

    const debouncedUpdateDimention = debounce(updateDimentionChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateDimention);
    regionSelect.addEventListener("change", debouncedUpdateDimention);
}

dimention_loadDataAndRenderChart();
"""


def generate_html_dimention(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_REGION_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds region_region_year_json
    documents = list(collection.find({"type": "dimention", "region": {"$ne": None}}).sort("year", 1))
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

    output_file = os.path.join(output_dir, "dimention_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds region_year_json
    documents = list(collection.find({"type": "dimention", "region": None}).sort("year", 1))
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

    output_file = os.path.join(output_dir, "dimention_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }