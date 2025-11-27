import logging
import json
from airflow.providers.mongo.hooks.mongo import MongoHook


HTML_TEMPLATE = """
const lang_region_year_json = {lang_region_year_json};
const lang_year_json = {lang_year_json};

Highcharts.setOptions({
    lang: {
        thousandsSep: ' '
    }
});

let lang_chart = Highcharts.chart("lang_container", {
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
        min: 1,
        title: { text: "Number of documents" },
        type: "logarithmic"
    },
    plotOptions: {
        bar: {
            dataLabels: { enabled: true },
        },
    },
    series: [{ name: "Number of documents", data: [], color: "#0093d5" }],
});

function updateLangChart() {
    const year_range = slider.noUiSlider.get(true);
    const yearFrom = parseInt(year_range[0]);
    const yearTo = parseInt(year_range[1]);
    if (yearFrom > yearTo) return;

    const selectedRegion = regionSelect.value;

    let year_from = {year_from};
    let filtered;
    if (selectedRegion === "Todas") {
        filtered = Object.values(lang_year_json).flat();
    } else {
        filtered = lang_region_year_json[selectedRegion];
    }
    // Selecting all years before starting year
    if (yearFrom === year_from) {
        filtered = filtered.filter((d) => d.ano <= yearTo);
    } else {
        filtered = filtered.filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
    }

    if (!filtered || filtered.length === 0) {
        let langs = Object.keys(Object.values(lang_year_json)[0][0]).filter((key) => key !== "ano");
        langs = langs.slice(0, 10);
        lang_chart.series[0].setData(langs.map(() => 0));
        lang_chart.update({ xAxis: { categories: langs } });
        return;
    }

    const langs = [...new Set(filtered.flatMap(obj => Object.keys(obj)))].filter(key => key !== "ano");

    const total = {};

    filtered.forEach((d) => {
        langs.forEach((lang) => {
            total[lang] = (total[lang] || 0) + (d[lang] || 0);
        });
    });

    // Monta pares idioma/valor
    let sorted = langs.map((lang) => ({
        name: lang,
        value: total[lang]
    }));

    // Ordena do maior para o menor
    sorted.sort((a, b) => b.value - a.value);
    sorted = sorted.slice(0, 10);

    // Atualiza gráfico com dados ordenados
    lang_chart.series[0].setData(sorted.map(item => item.value));
    lang_chart.update({ xAxis: { categories: sorted.map(item => item.name) } });
}

const debouncedUpdateLang = debounce(updateLangChart, 100);
slider.noUiSlider.on("update", debouncedUpdateLang);
regionSelect.addEventListener("change", debouncedUpdateLang);
"""


def generate_html_language(year_from):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Builds lang_region_year_json
    documents = list(collection.find({"type": "language", "region": {"$ne": None}}).sort("year", 1))
    aggregated_data = {}
    regions = set()
    for doc in documents:
        region = doc["region"]
        regions.add(region)

        year = int(doc["year"])
        lang = doc["name"]
        count = doc.get("count", 0)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data[region].append(year_data)

        year_data[lang] = count

    lang_region_year_json = json.dumps(aggregated_data, ensure_ascii=False)


    # Builds lang_year_json
    documents = list(collection.find({"type": "language", "region": None}).sort("year", 1))
    aggregated_data = []
    years = []
    for doc in documents:
        year = int(doc["year"])
        lang = doc["name"]
        count = doc.get("count", 0)
        years.append(year)

        year_data = next((item for item in aggregated_data if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            aggregated_data.append(year_data)

        year_data[lang] = count

    lang_year_json = json.dumps(aggregated_data, ensure_ascii=False)


    min_year = min(years)
    max_year = max(years)

    region_options = "\n".join(
        f'<option value="{r}">{r}</option>' for r in sorted(regions)
    )

    html_with_data = HTML_TEMPLATE.replace("{lang_region_year_json}", lang_region_year_json)
    html_with_data = html_with_data.replace("{lang_year_json}", lang_year_json)
    html_with_data = html_with_data.replace("{year_from}", str(year_from))

    return { 
        'min_year': min_year,
        'max_year': max_year,
        'region_options': region_options,
        'html': html_with_data
    }