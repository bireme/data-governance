import os
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="UTF-8" />
  <title>TM Research Analytics</title>
  <script src="https://code.highcharts.com/highcharts.js"></script>
  <script src="https://code.highcharts.com/modules/exporting.js"></script>
  <link
    rel="stylesheet"
    href="https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.8.1/nouislider.min.css"
  />
  <script src="https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.8.1/nouislider.min.js"></script>
  <style>
    body {{
      font-family: Arial, sans-serif;
      padding: 20px;
    }}
    #container {{
      width: 100%;
      max-width: 900px;
      margin: 30px auto;
    }}
    .slider-control {{
      max-width: 900px;
      margin: 15px auto;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 15px;
    }}
    input[type=range] {{
      width: 200px;
    }}
    #yearRangeSlider {{
      width: 300px;
      margin-bottom: 8px;
    }}
  </style>
</head>
<body>
  <h2>TM Research Analytics</h2>

  <div class="slider-control">
    <label for="regionSelect">WHO Region</label>
    <select id="regionSelect">
      <option value="Todas">Todas</option>
      {region_options}
    </select>
  </div>

  <div class="slider-control">
    <div id="yearRangeSlider"></div>
  </div>

  <div id="container"></div>

  <script>
    const slider = document.getElementById("yearRangeSlider");
    const regionSelect = document.getElementById("regionSelect");

    noUiSlider.create(slider, {{
      start: [{year_range_min}, {year_range_max}],
      connect: true,
      range: {{ min: {year_range_min}, max: {year_range_max} }},
      step: 1,
      tooltips: true,
      format: {{
        to: (value) => Math.floor(value),
        from: (value) => Number(value),
      }},
    }});

    const dataJSON = {data_json};

    let chart = Highcharts.chart("container", {{
      chart: {{ type: "bar" }},
      title: {{ text: "Comparativo de Idiomas" }},
      legend: {{ enabled: false }},
      xAxis: {{ 
        title: {{ text: null }},
        labels: {{
          rotation: 0,
          step: 1,
          style: {{
            fontSize: '11px'
          }}
        }}
      }},
      yAxis: {{
        min: 0,
        title: {{ text: "Documentos" }},
      }},
      tooltip: {{
        valueSuffix: " registros",
      }},
      plotOptions: {{
        bar: {{
          dataLabels: {{ enabled: true }},
        }},
      }},
      series: [{{ name: "Documentos", data: [] }}],
    }});

    function updateChart() {{
      const year_range = slider.noUiSlider.get(true);
      const yearFrom = parseInt(year_range[0]);
      const yearTo = parseInt(year_range[1]);
      if (yearFrom > yearTo) return;

      const selectedRegion = regionSelect.value;

      let filtered;

      if (selectedRegion === "Todas") {{
        filtered = Object.values(dataJSON)
          .flat()
          .filter((d) => d.ano >= yearFrom && d.ano <= yearTo);
      }} else {{
        filtered = dataJSON[selectedRegion].filter(
          (d) => d.ano >= yearFrom && d.ano <= yearTo
        );
      }}

      if (!filtered || filtered.length === 0) {{
        const langs = Object.keys(Object.values(dataJSON)[0][0]).filter((key) => key !== "ano");
        chart.series[0].setData(langs.map(() => 0));
        chart.update({{ xAxis: {{ categories: langs }} }});
        return;
      }}

      const langs = Object.keys(filtered[0]).filter((key) => key !== "ano" && filtered.some(d => d[key] > 0));

      const total = {{}};
      langs.forEach((lang) => (total[lang] = 0));

      filtered.forEach((d) => {{
        langs.forEach((lang) => {{
          total[lang] += d[lang] || 0;
        }});
      }});

      // Monta pares idioma/valor
      let sorted = langs.map((lang) => ({{
        name: lang,
        value: total[lang]
      }}));

      // Ordena do maior para o menor
      sorted.sort((a, b) => b.value - a.value);
      sorted = sorted.slice(0, 10);

      // Atualiza gráfico com dados ordenados
      chart.series[0].setData(sorted.map(item => item.value));
      chart.update({{ xAxis: {{ categories: sorted.map(item => item.name) }} }});

      const titleRegionText = selectedRegion === "Todas" ? "Todas as Regiões" : selectedRegion;

      chart.setTitle({{
        text: `Distribuição de documentos por idiomas`,
      }});
    }}

    slider.noUiSlider.on("update", updateChart);
    regionSelect.addEventListener("change", updateChart);

    updateChart();
  </script>
</body>
</html>
"""

def generate_html_reports():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    documents = list(collection.find({"type": "language"}))
    all_langs = set(doc["name"] for doc in documents)

    aggregated_data = {}
    years = []
    regions = set()

    for doc in documents:
        region = doc["region"]
        regions.add(region)

        year = int(doc["year"])
        lang = doc["name"]
        count = doc.get("count", 0)
        logger.info(doc["year"])
        logger.info(year)
        years.append(year)

        if region not in aggregated_data:
            aggregated_data[region] = []

        year_data = next((item for item in aggregated_data[region] if item["ano"] == year), None)
        if not year_data:
            year_data = {"ano": year}
            for l in all_langs:
                year_data[l] = 0
            aggregated_data[region].append(year_data)

        year_data[lang] = count

    for reg in aggregated_data:
        aggregated_data[reg] = sorted(aggregated_data[reg], key=lambda x: x["ano"])

    min_year = min(years)
    max_year = max(years)
    logger.info(years)
    logger.info(min_year)
    logger.info(max_year)

    dynamic_data_json = json.dumps(aggregated_data, ensure_ascii=False)

    region_options = "\n".join(
        f'<option value="{r}">{r}</option>' for r in sorted(regions)
    )

    html_with_data = HTML_TEMPLATE.format(
        data_json=dynamic_data_json,
        region_options=region_options,
        year_range_min=min_year,
        year_range_max=max_year,
    )

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()
    output_file = os.path.join(output_dir, "report_languages.html")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_with_data)

    logger.info(f"HTML report gerado e salvo em {output_file}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'TMGL_REGION_03_export_html',
    default_args=default_args,
    description='TMGL - Exporta HTML com métricas de documentos por região',
    schedule_interval=None,
    catchup=False,
    tags=['tmgl', 'report', 'html']
) as dag:
    generate_reports_task = PythonOperator(
        task_id='generate_html_reports',
        python_callable=generate_html_reports
    )
