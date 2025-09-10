import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from data_governance.dags.tmgl_regions.tasks_for_export.map import generate_html_map
from data_governance.dags.tmgl_regions.tasks_for_export.language import generate_html_language
from data_governance.dags.tmgl_regions.tasks_for_export.timeline import generate_html_timeline
from data_governance.dags.tmgl_regions.tasks_for_export.indicator import generate_html_indicators
from data_governance.dags.tmgl_regions.tasks_for_export.journal import generate_html_journal


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="UTF-8" />
  <title>TM Research Analytics</title>

  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-sRIl4kxILFvY47J16cr9ZwB07vP4J8+LH7qKQnuqkuIAvNWLzeN8tE5YBujZqJLB" crossorigin="anonymous">
  <style>@import url('https://fonts.googleapis.com/css2?family=Roboto:ital,wght@0,100..900;1,100..900&display=swap');</style>
  <link href="tmgl_regions.css" rel="stylesheet">

  <script src="./highcharts.js"></script>
  <script src="https://code.highcharts.com/modules/no-data-to-display.js"></script>
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

  <ul class="nav nav-pills nav-justified custom-nav my-3" id="pills-tab" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="pills-output-tab" data-bs-toggle="pill" data-bs-target="#output-tab-pane" type="button" role="tab" aria-controls="output-tab-pane" aria-selected="true">Global Scientific Output</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-study-type-tab" data-bs-toggle="pill" data-bs-target="#study-type-tab-pane" type="button" role="tab" aria-controls="study-type-tab-pane" aria-selected="false">Study Type and Sources</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-topics-countries-tab" data-bs-toggle="pill" data-bs-target="#topics-countries-tab-pane" type="button" role="tab" aria-controls="topics-countries-tab-pane" aria-selected="false">Topics & Countries focus</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-tcim-areas-tab" data-bs-toggle="pill" data-bs-target="#tcim-areas-tab-pane" type="button" role="tab" aria-controls="tcim-areas-tab-pane" aria-selected="false">TCIM areas</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-about-tab" data-bs-toggle="pill" data-bs-target="#about-tab-pane" type="button" role="tab" aria-controls="about-tab-pane" aria-selected="false">About</button>
    </li>
  </ul>

  <div class="d-flex justify-content-center" id="filters">
    <div class="slider-control form-floating">
      <select id="regionSelect" class="form-select">
        <option value="Todas">All</option>
        {region_options}
      </select>
      <label for="regionSelect">WHO Region</label>
    </div>

    <div class="slider-control pt-1 ms-2">
      <div id="yearRangeSlider"></div>
    </div>
  </div>

  <div class="tab-content">
    <div class="tab-pane fade show active" id="output-tab-pane" role="tabpanel" aria-labelledby="pills-output-tab">
      <div class="row mt-4">
        <h3 class="h4">Total Publications and Full-Text Availability by Country</h3>
        <div class="row m-0">
          <div class="col-lg-6 col-xs-12">
            <div id="map_container"></div>
          </div>

          <div class="col-lg-6 col-xs-12 mt-lg-0 mt-3">
            <div id="indicator_container" class="py-5">
              <div class="d-flex justify-content-center text-center mt-5">
                <div class="p-2" style="flex: 1 1 50%;">
                  Total Documents<br><span id="indicator_total_documents"></span>
                </div>
                <div class="p-2" style="flex: 1 1 50%;">
                  Full Text<br><span id="indicator_total_fulltext"></span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="row mt-4">
        <div class="col-lg-6 col-xs-12">
          <h3 class="h4">Publications by Language</h3>
          <div id="lang_container"></div>
        </div>

        <div class="col-lg-6 col-xs-12 mt-lg-0 mt-3">
          <h3 class="h4">Total Publications and Full-Text Availability over time</h3>
          <div id="timeline_container"></div>
        </div>
      </div>
    </div>


    <div class="tab-pane fade" id="study-type-tab-pane" role="tabpanel" aria-labelledby="pills-study-type-tab">
      <div class="row mt-4">
        <div class="col-lg-6 col-xs-12">
          <h3 class="h4">Top 10 Journals</h3>
          <div id="journals_container"></div>

          <h3 class="h4 mt-3">Publications by Document Type</h3>
          <div id="document_type_container"></div>
        </div>

        <div class="col-lg-6 col-xs-12 mt-lg-0 mt-3">
          <h3 class="h4">Publications by Study Type</h3>
          <div id="study_type_container"></div>
        </div>
      </div>
    </div>


    <div class="tab-pane fade" id="about-tab-pane" role="tabpanel" aria-labelledby="pills-about-tab">
      <div class="row mt-4">
        <div class="col-xs-12">
          <h3 class="h4">About</h3>
          <p><strong>TM Research Analytics</strong> is an interactive tool designed to support the analysis, visualization, and interpretation of global scientific output related to <strong>Traditional, Complementary, and Integrative Medicine (TCIM)</strong>.</p>
          <p>Its purpose is to provide researchers, policymakers, managers, and other stakeholders with a comprehensive overview of trends, geographical distribution, therapeutic methods, and key thematic areas in this field, facilitating evidence-informed decision-making.</p>
          <p>The dashboard was developed based on data indexed in the <strong>Traditional Medicine Global Library (TMGL)</strong> and related databases. The process involved data extraction, cleaning, and classification, followed by the creation of interactive visualizations using <strong>Python</strong>. The design and indicators were defined collaboratively by BIREME/PAHO/WHO in alignment with the needs of the WHO Traditional Medicine Centre (TMC) and the Department of Digital Health and Innovation (DHI).</p>
        </div>
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

    {html_indicators}

    {html_journals}
  </script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/js/bootstrap.bundle.min.js" integrity="sha384-FKyoEForCGlyvwx9Hj09JcYn3nv7wiPVlz7YYwJrWVcXK/BmnVDxM+D2scQbITxI" crossorigin="anonymous"></script>
</body>
</html>
"""


def generate_html_reports(ti):
    logger = logging.getLogger(__name__)

    language_data = ti.xcom_pull(task_ids='generate_html_language')
    timeline_data = ti.xcom_pull(task_ids='generate_html_timeline')
    map_data = ti.xcom_pull(task_ids='generate_html_map')
    indicators_data = ti.xcom_pull(task_ids='generate_html_indicators')
    journal_data = ti.xcom_pull(task_ids='generate_html_journal')

    html_with_data = HTML_TEMPLATE.format(
        html_language=language_data['html'],
        region_options=language_data['region_options'],
        year_range_min=language_data['min_year'],
        year_range_max=language_data['max_year'],
        html_timeline=timeline_data['html'],
        html_map=map_data['html'],
        html_indicators=indicators_data['html'],
        html_journals=journal_data['html']
    )

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()
    output_file = os.path.join(output_dir, "index.html")

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
    generate_html_indicators_task = PythonOperator(
        task_id='generate_html_indicators',
        python_callable=generate_html_indicators,
    )
    generate_html_journal_task = PythonOperator(
        task_id='generate_html_journal',
        python_callable=generate_html_journal,
    )

    generate_reports_task = PythonOperator(
        task_id='generate_html_reports',
        python_callable=generate_html_reports
    )

    generate_html_language_task >> generate_reports_task
    generate_html_timeline_task >> generate_reports_task
    generate_html_map_task >> generate_reports_task
    generate_html_indicators_task >> generate_reports_task
    generate_html_journal_task >> generate_reports_task