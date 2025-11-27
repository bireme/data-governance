import logging
import json
import os
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
async function region_loadDataAndRenderChart() {
    const regionResp = await fetch('region_region_year.json');
    const region_region_year_json = await regionResp.json();

    const yearResp = await fetch('region_year.json');
    const region_year_json = await yearResp.json();

    Highcharts.setOptions({
        lang: {
            thousandsSep: ' '
        }
    });

    let region_chart = Highcharts.chart("region_container", {
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
            name: "WHO Regions", 
            data: [], 
        }],
        drilldown: {
            series: []
        }
    });

    function updateRegionChart() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let year_from = {year_from};
        let filtered;
        if (selectedRegion === "Todas") {
            filtered = Object.values(region_year_json).flat();
        } else {
            filtered = region_region_year_json[selectedRegion];
        }
        // Selecting all years before starting year
        if (yearFrom === year_from) {
            filtered = filtered.filter((d) => d.ano <= yearTo);
        } else {
            filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
        }

        if (!filtered || filtered.length === 0) {
            region_chart.series[0].setData([]);
            region_chart.showNoData();
            return;
        } else {
            region_chart.hideNoData();
        }

        const regions = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

        const total = {};

        filtered.forEach((d) => {
            regions.forEach((region) => {
                total[region] = (total[region] || 0) + (d[region] || 0);
            });
        });

        // Monta pares idioma/valor
        let sorted = regions.map((region) => ({
            name: region,
            y: total[region]
        }));

        // Ordena do maior para o menor
        sorted.sort((a, b) => b.y - a.y);

        const regionDataMap = {};
        const drilldownMap = {};

        sorted.forEach(({name, y}) => {
            const [region, country] = name.split('/');
            if (!regionDataMap[region]) {
                regionDataMap[region] = 0;
                drilldownMap[region] = {
                    name: region,
                    id: region,
                    data: []
                };
            }
            regionDataMap[region] += y;
            drilldownMap[region].data.push([country, y]);
        });

        // Criar o array data com soma dos valores por região
        const data = Object.entries(regionDataMap).map(([region, sum]) => ({
            name: region.toUpperCase(),
            y: sum,
            drilldown: region
        }));

        const drilldown = {
            series: Object.values(drilldownMap)
        };

        // Atualiza gráfico com dados ordenados
        region_chart.update({
            drilldown: {
                series: drilldown.series
            }
        }, false);

        region_chart.series[0].setData(data);
    }

    const debouncedUpdateRegion = debounce(updateRegionChart, 100);
    slider.noUiSlider.on("update", debouncedUpdateRegion);
    regionSelect.addEventListener("change", debouncedUpdateRegion);
}

region_loadDataAndRenderChart();
"""


def generate_html_region(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    fs_hook = FSHook(fs_conn_id='TMGL_REGION_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Builds region_region_year_json
    documents = list(collection.find({"type": "region", "region": {"$ne": None}}).sort("year", 1))
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

    output_file = os.path.join(output_dir, "region_region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    # Builds region_year_json
    documents = list(collection.find({"type": "region", "region": None}).sort("year", 1))
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

    output_file = os.path.join(output_dir, "region_year.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)


    html_with_data = HTML_TEMPLATE.replace("{year_from}", str(year_from))

    return { 
        'html': html_with_data
    }