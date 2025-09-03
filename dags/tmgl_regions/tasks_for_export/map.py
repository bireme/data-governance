import logging
import json
from airflow.providers.mongo.hooks.mongo import MongoHook


HTML_TEMPLATE = """
const map_json = {map_json};

(async () => {

    const topology = await fetch(
            './world.topo.json'
        ).then(response => response.json());

    let map = Highcharts.mapChart('map_container', {
        chart: {
            map: topology,
            backgroundColor: '#F7F7F8',
            borderRadius: 16,
            borderColor: '#C7C6C0',
            borderWidth: 2,
            spacingTop: 20
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
            text: ''
        },
        mapNavigation: {
            enabled: true,
            buttonOptions: {
                verticalAlign: 'bottom'
            }
        },
        colorAxis: {
            min: 0,
            minColor: '#a7d3ff',
            maxColor: '#001523',
            labels: {
                format: '{value}'
            }
        },
        series: [{
            name: 'Total Documents',
            joinBy: ['iso-a2', 'country_iso'],
            mapData: Highcharts.maps['custom/world'],
            data: [],
            //borderColor: '#FFFFFF',
            borderWidth: 0.5,
            nullColor: '#F8F8F8',
            states: {
                hover: {
                    color: '#FFA500'
                }
            },
            tooltip: {
                pointFormatter: function () {
                    return `<b>${this.name}</b><br>` +
                        `Total Documents: ${Highcharts.numberFormat(this.value || 0, 0, ',', '.')}<br>` +
                        `Full Texts: ${Highcharts.numberFormat(this.total_fulltext || 0, 0, ',', '.')}`;
                }
            }
        }]
    });

    function updateMap() {
        const year_range = slider.noUiSlider.get(true);
        const yearFrom = parseInt(year_range[0]);
        const yearTo = parseInt(year_range[1]);
        if (yearFrom > yearTo) return;

        const selectedRegion = regionSelect.value;

        let filteredData;

        if (selectedRegion === "Todas") {
            filteredData = Object.values(map_json)
                .flat()
                .filter(d => d.ano >= yearFrom && d.ano <= yearTo);
        } else {
            filteredData = map_json[selectedRegion]?.filter(d => d.ano >= yearFrom && d.ano <= yearTo) ?? [];
        }

        if (!filteredData || filteredData.length === 0) {
            map.series[0].setData([]);
            return;
        }

        const aggregated = {};

        filteredData.forEach(d => {
            if (!aggregated[d.country_iso]) {
                aggregated[d.country_iso] = {
                    country_iso: d.country_iso,
                    name: d.country_name,
                    total_documents: 0,
                    total_fulltext: 0
                };
            }
            aggregated[d.country_iso].total_documents += d.total_documents || 0;
            aggregated[d.country_iso].total_fulltext += d.total_fulltext || 0;
        });

        // Montar o array com os dados no formato esperado pelo Highcharts
        const dataForMap = Object.values(aggregated).map(d => ({
            country_iso: d.country_iso,
            value: d.total_documents,      // para cor no colorAxis
            total_fulltext: d.total_fulltext,
            name: d.name
        }));

        // Atualizar colorAxis para o máximo valor, para calibrar cor
        const maxDocuments = Math.max(...dataForMap.map(d => d.value));
        map.update({
            colorAxis: {
                max: maxDocuments
            }
        });

        map.series[0].setData(dataForMap, true, { duration: 500 });
    }

    const debouncedUpdateMap = debounce(updateMap, 100);
    slider.noUiSlider.on('update', debouncedUpdateMap);
    regionSelect.addEventListener('change', debouncedUpdateMap);

    updateMap();
})();
"""


def generate_html_map():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    documents = list(collection.find({"type": "map"}))

    aggregated_data = {}

    for doc in documents:
        region = doc["region"]
        country_name = doc["country_name"]
        country_iso = doc["country_iso"]
        year = int(doc["year"])
        total_documents = doc.get("total", 0)
        total_fulltext = doc.get("with_fulltext", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = {
            "ano": year,
            "country_name": country_name,
            "country_iso": country_iso,
            "total_documents": total_documents,
            "total_fulltext": total_fulltext
        }
        aggregated_data[region].append(year_data)

    for reg in aggregated_data:
        aggregated_data[reg] = sorted(aggregated_data[reg], key=lambda x: x["ano"])

    data_json = json.dumps(aggregated_data, ensure_ascii=False)

    html_with_data = HTML_TEMPLATE.replace("{map_json}", data_json)

    return { 
        'html': html_with_data
    }