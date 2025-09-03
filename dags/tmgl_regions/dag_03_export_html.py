import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from data_governance.dags.tmgl_regions.tasks_for_export.map import generate_html_map
from data_governance.dags.tmgl_regions.tasks_for_export.language import generate_html_language
from data_governance.dags.tmgl_regions.tasks_for_export.timeline import generate_html_timeline


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="UTF-8" />
  <title>TM Research Analytics</title>

  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-sRIl4kxILFvY47J16cr9ZwB07vP4J8+LH7qKQnuqkuIAvNWLzeN8tE5YBujZqJLB" crossorigin="anonymous">
  <style>@import url('https://fonts.googleapis.com/css2?family=Roboto:ital,wght@0,100..900;1,100..900&display=swap');</style>
  <style>
    body {{
      font-family: "Roboto", sans-serif !important;
      padding: 20px;
    }}
    h2 {{
      font-size: 26px;
      font-weight: 800;
    }}
    h2 img {{
      height: 28px;
      vertical-align: top;
    }}
    h3 {{
      font-weight: 700 !important;
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
    .custom-nav {{
      background: #F3F3F2;
      border-radius: 16px;
      font-size: 20px !important;
      padding: 8px 0;
    }}
    .custom-nav .nav-link {{
      color: #222;
      border-radius: 16px;
      text-align: center;
    }}
    .custom-nav .selected {{
      color: #0093d5;
      font-weight: 800;
    }}
  </style>

  <script src="./highcharts.js"></script>
  <script src="./map.js"></script>
  <script src="./accessibility.js"></script>
  <script src="./exporting.js"></script>
  <link
    rel="stylesheet"
    href="https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.8.1/nouislider.min.css"
  />
  <script src="https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.8.1/nouislider.min.js"></script>
</head>
<body>
  <h2><img src="icone_tmgl.svg"> TM Research Analytics</h2>

  <ul class="nav nav-pills nav-justified custom-nav my-3">
    <li class="nav-item">
      <a class="nav-link selected" href="#">Global Scientific Output</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#">Study Type and Sources</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#">Topics & Countries focus</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#">TCIM areas</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#">About</a>
    </li>
  </ul>

  <div class="slider-control">
    <div id="yearRangeSlider"></div>
  </div>

  <div class="slider-control">
    <label for="regionSelect">WHO Region</label>
    <select id="regionSelect">
      <option value="Todas">Todas</option>
      {region_options}
    </select>
  </div>

  <div class="row">
    <div class="col-lg-6 col-xs-12">
      <div class="mt-3">
        <h3 class="h4">Total Publications and Full-Text Availability by Country</h3>
        <div id="map_container"></div>
      </div>

      <div class="mt-3">
        <h3 class="h4">Publications by Language</h3>
        <div id="lang_container"></div>
      </div>
    </div>

    <div class="col-lg-6 col-xs-12">
      <div class="mt-3">
        <h3 class="h4">Total Publications and Full-Text Availability over time</h3>
        <div id="timeline_container"></div>
      </div>
    </div>
  </div>

  <script>
    function debounce(fn, delay) {{
      let timer = null;
      return function(...args) {{
        clearTimeout(timer);
        timer = setTimeout(() => fn.apply(this, args), delay);
      }};
    }}

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

    {html_map}

    {html_language}

    {html_timeline}
  </script>
</body>
</html>
"""


def generate_html_reports(ti):
    logger = logging.getLogger(__name__)

    language_data = ti.xcom_pull(task_ids='generate_html_language')
    timeline_data = ti.xcom_pull(task_ids='generate_html_timeline')
    map_data = ti.xcom_pull(task_ids='generate_html_map')

    html_with_data = HTML_TEMPLATE.format(
        html_language=language_data['html'],
        region_options=language_data['region_options'],
        year_range_min=language_data['min_year'],
        year_range_max=language_data['max_year'],
        html_timeline=timeline_data['html'],
        html_map=map_data['html']
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
    generate_html_language_task = PythonOperator(
        task_id='generate_html_language',
        python_callable=generate_html_language,
    )
    generate_html_timeline_task = PythonOperator(
        task_id='generate_html_timeline',
        python_callable=generate_html_timeline,
    )
    generate_html_map_task = PythonOperator(
        task_id='generate_html_map',
        python_callable=generate_html_map,
    )

    generate_reports_task = PythonOperator(
        task_id='generate_html_reports',
        python_callable=generate_html_reports
    )

    generate_html_language_task >> generate_reports_task
    generate_html_timeline_task >> generate_reports_task
    generate_html_map_task >> generate_reports_task