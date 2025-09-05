import logging
import json
from airflow.providers.mongo.hooks.mongo import MongoHook


HTML_TEMPLATE = """
const timeline_region_year_json = {timeline_region_year_json};
const timeline_year_json = {timeline_year_json};

let timeline_chart = Highcharts.chart("timeline_container", {
    chart: { 
        type: "line",
        backgroundColor: '#F7F7F8',
        borderRadius: 16,
        borderColor: '#C7C6C0',
        borderWidth: 2,
        spacingTop: 30,
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
    xAxis: { 
        title: { text: "Year" }
    },
    yAxis: {
        min: 0,
        title: { text: "Total" },
    },
    plotOptions: {
        line: {
            dataLabels: { enabled: true },
        },
    },
    series: [
        { name: "Total Documents", data: [], color: "#0093d5" },
        { name: "Full Texts", data: [], color: "#005881" }
    ]
});

function updateTimelineChart() {
    const year_range = slider.noUiSlider.get(true);
    const yearFrom = parseInt(year_range[0]);
    const yearTo = parseInt(year_range[1]);
    if (yearFrom > yearTo) return;

    const selectedRegion = regionSelect.value;

    let filtered;

    if (selectedRegion === "Todas") {
        // Junta dados de todas as regiões
        filtered = Object.values(timeline_year_json)
            .flat()
            .filter((d) => d.ano >= yearFrom && d.ano <= yearTo);

        // Agrupa por ano somando os valores
        const grouped = {};
        filtered.forEach(d => {
            if (!grouped[d.ano]) {
                grouped[d.ano] = {
                    ano: d.ano, 
                    total_documents: 0, 
                    total_fulltext: 0 
                };
            }
            grouped[d.ano].total_documents += d.total_documents || 0;
            grouped[d.ano].total_fulltext += d.total_fulltext || 0;
        });

        // Converte para array
        filtered = Object.values(grouped);
    } else {
        filtered = timeline_region_year_json[selectedRegion]?.filter(
            (d) => d.ano >= yearFrom && d.ano <= yearTo
        ) ?? [];
    }

    if (!filtered || filtered.length === 0) {
        timeline_chart.series[0].setData([]);
        timeline_chart.series[1].setData([]);
        timeline_chart.update({ xAxis: { categories: [] } });
        return;
    }

    // Ordena pelos anos
    filtered.sort((a, b) => a.ano - b.ano);

    const anos = filtered.map(d => d.ano);
    const total_documents = filtered.map(d => d.total_documents);
    const total_fulltext = filtered.map(d => d.total_fulltext);

    timeline_chart.series[0].setData(total_documents);
    timeline_chart.series[1].setData(total_fulltext);
    timeline_chart.update({ xAxis: { categories: anos } });
}

const debouncedUpdateTimeline = debounce(updateTimelineChart, 100);
slider.noUiSlider.on("update", debouncedUpdateTimeline);
regionSelect.addEventListener("change", debouncedUpdateTimeline);

updateTimelineChart();
"""


def generate_html_timeline():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Documents with region
    documents = list(collection.find({"type": "timeline", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    for doc in documents:
        region = doc["region"]

        year = int(doc["year"])
        total_documents = doc.get("total", 0)
        total_fulltext = doc.get("with_fulltext", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = {
            "ano": year,
            "total_documents": total_documents,
            "total_fulltext": total_fulltext
        }
        aggregated_data[region].append(year_data)

    timeline_region_year_json = json.dumps(aggregated_data, ensure_ascii=False)


    # Documents without region
    documents = list(collection.find({"type": "timeline", "region": None}).sort("year", 1))
    aggregated_data = []
    for doc in documents:
        year = int(doc["year"])
        total_documents = doc.get("total", 0)
        total_fulltext = doc.get("with_fulltext", 0)

        year_data = {
            "ano": year,
            "total_documents": total_documents,
            "total_fulltext": total_fulltext
        }
        aggregated_data.append(year_data)

    timeline_year_json = json.dumps(aggregated_data, ensure_ascii=False)


    html_with_data = HTML_TEMPLATE.replace("{timeline_region_year_json}", timeline_region_year_json)
    html_with_data = html_with_data.replace("{timeline_year_json}", timeline_year_json)

    return { 
        'html': html_with_data
    }