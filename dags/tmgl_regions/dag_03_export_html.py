import os
import logging
import shutil
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from data_governance.dags.tmgl_regions.tasks_for_export.map import generate_html_map
from data_governance.dags.tmgl_regions.tasks_for_export.language import generate_html_language
from data_governance.dags.tmgl_regions.tasks_for_export.timeline import generate_html_timeline
from data_governance.dags.tmgl_regions.tasks_for_export.indicator import generate_html_indicators
from data_governance.dags.tmgl_regions.tasks_for_export.journal import generate_html_journal
from data_governance.dags.tmgl_regions.tasks_for_export.doctype import generate_html_doctype
from data_governance.dags.tmgl_regions.tasks_for_export.studytype import generate_html_studytype
from data_governance.dags.tmgl_regions.tasks_for_export.subject import generate_html_subject
from data_governance.dags.tmgl_regions.tasks_for_export.region import generate_html_region
from data_governance.dags.tmgl_regions.tasks_for_export.dimention import generate_html_dimention
from data_governance.dags.tmgl_regions.tasks_for_export.therapies import generate_html_therapy
from data_governance.dags.tmgl_regions.tasks_for_export.complementary import generate_html_complementary
from data_governance.dags.tmgl_regions.tasks_for_export.traditional import generate_html_traditional


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
  <script src="./no-data-to-display.js"></script>
  <script src="./map.js"></script>
  <script src="./wordcloud.js"></script>
  <script src="./drilldown.js"></script>
  <script src="./treemap.js"></script>
  <script src="./highcharts-more.js"></script>
  <script src="./dumbbell.js"></script>
  <script src="./lollipop.js"></script>
  <script src="./accessibility.js"></script>
  <script src="./exporting.js"></script>
  <script src="./export-data.js"></script>
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
      <button class="nav-link" id="pills-topics-countries-tab" data-bs-toggle="pill" data-bs-target="#topics-countries-tab-pane" type="button" role="tab" aria-controls="topics-countries-tab-pane" aria-selected="false">Topics & Countries</button>
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
          <div id="doctype_container"></div>
        </div>

        <div class="col-lg-6 col-xs-12 mt-lg-0 mt-3">
          <h3 class="h4">Publications by Study Type</h3>
          <div id="studytype_container"></div>
        </div>
      </div>
    </div>


    <div class="tab-pane fade" id="topics-countries-tab-pane" role="tabpanel" aria-labelledby="pills-topics-countries-tab">
      <div class="row mt-4">
        <div class="col-lg-6 col-xs-12">
          <h3 class="h4">Most Frequent Subjects</h3>
          <div id="subject_container"></div>

          <h3 class="h4 mt-3">TCIM Publications by WHO region with country as topic</h3>
          <div id="region_container"></div>
        </div>

        <div class="col-lg-6 col-xs-12 mt-lg-0 mt-3">
          <h3 class="h4">Publications by Traditional Medicine Dimension</h3>
          <div id="dimention_container"></div>
        </div>
      </div>
    </div>


    <div class="tab-pane fade" id="tcim-areas-tab-pane" role="tabpanel" aria-labelledby="pills-tcim-areas-tab">
      <div class="row mt-4">
        <div class="col-lg-6 col-xs-12">
          <h3 class="h4">Therapeutic Methods and Therapies distribution</h3>
          <div id="therapy_container"></div>

          <h3 class="h4 mt-3">Complementary Medicines Distribution</h3>
          <div id="complementary_container"></div>
        </div>

        <div class="col-lg-6 col-xs-12 mt-lg-0 mt-3">
          <h3 class="h4">Traditional Medicines</h3>
          <div id="traditional_container"></div>
        </div>
      </div>
    </div>


    <div class="tab-pane fade" id="about-tab-pane" role="tabpanel" aria-labelledby="pills-about-tab">
      <div class="row mt-4">
        <div class="col-xs-12">
          <h3 class="h4">About</h3>
          <p><strong>TM Research Analytics</strong> is an interactive dashboard developed by <a href="https://www.paho.org/en/bireme">BIREME/PAHO/WHO</a> to support the analysis, visualization, and interpretation of global and WHO regional scientific outputs in the field of Traditional, Complementary, and Integrative Medicine (TCIM).</p>
          <p>The dashboard integrates bibliographic records indexed in the Traditional Medicine Global Library (TMGL) mega-database, which compiles scientific literature collected, indexed, or archived from various organizations. Data are accessible through the integrated search interface (iAHx).</p>
          <p>It provides 13 core bibliometric and infometric indicators that describe the evolution, distribution, and thematic orientation of TCIM research. These indicators enable comprehensive analysis of publication volume, growth dynamics, geographic coverage, thematic trends, and research methodologies.</p>
          <p>
            <strong>Analytical dimensions include</strong>
            <ul>
              <li><strong>Scientific Production:</strong> total documents, full-text availability, and temporal trends over the last decade.</li>
              <li><strong>Document and Study Types:</strong> classification of research outputs and identification of prevailing methodologies.</li>
              <li><strong>Journals:</strong> leading publication sources and dissemination channels.</li>
              <li><strong>Languages of Publication:</strong> linguistic diversity of TCIM outputs.</li>
              <li><strong>Subjects and Themes:</strong> most frequent descriptors reflecting research areas of concentration.</li>
              <li><strong>Traditional Medicine Dimensions:</strong> research classified by conceptual, therapeutic, and policy dimensions, inspired by the <strong>Gujarat Declaration</strong>.</li>
              <li><strong>Therapeutic Methods and TCIM Areas:</strong> representation of specific treatment approaches and thematic clusters.</li>
              <li><strong>Geographical Distribution:</strong> visualization of research activity across <strong>WHO Regions</strong>, supporting comparative and collaboration analyses.</li>
            </ul>
          </p>
          <p>
            <strong>Technical specifications</strong>
            <ul>
              <li>Data source: <strong>TMGL</strong> mega-database.</li>
              <li>Data processing pipeline includes extraction, cleaning, classification, and harmonization.</li>
              <li>Interactive visualizations implemented using <strong>Highcharts</strong>.</li>
              <li>Automated <strong>weekly updates</strong> synchronized with TMGL.</li>
              <li>Filters available by WHO Region and year of publication.</li>
            </ul>
          </p>
          <p>
            <strong>Legend — Study Types (Groupings)</strong>
            <ul>
              <li><strong>Systematic review</strong> = Systematic review + Systematic review of observational studies</li>
              <li><strong>Other Reviews</strong> = Literature review + Review</li>
              <li><strong>Other studies</strong> = Diagnostic, Etiology, Prognostic, Prevalence, Screening, Incidence, Health technology assessment, Health economic evaluation, Evaluation study, Overview/Evidence synthesis</li>
            </ul>
          </p>
          <p>
            <strong>Legend - Document Types</strong>
            <ul>
              <li><strong>Multimedia</strong> = Video + Audio</li>
            </ul>
          </p>
          <p>
            <strong>Legend - Publications by Language</strong>
            <ul>
              <li><strong>Mult</strong> = Multiple languages</li>
            </ul>
          </p>
          <p><strong>How to cite: BIREME/PAHO/WHO.</strong> <em>The WHO Traditional Medicine Global Library (TMGL)</em> [Internet]. São Paulo: BIREME/PAHO/WHO; [Year] [cited YYYY Mon DD]. Available from: <a href="https://tmgl.org">https://tmgl.org</a></p>
          <p>Last data update: {today}</p>
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
        to: (value) => {{
            const val = Math.floor(value);
            if (val === {year_range_min}) {{
                return "+ " + val;
            }}
            return val;
        }},
        from: (value) => Number(value)
      }},
    }});

    {html_map}

    {html_language}

    {html_timeline}

    {html_indicators}

    {html_journals}

    {html_doctype}

    {html_studytype}

    {html_subject}

    {html_region}

    {html_dimention}

    {html_therapy}

    {html_complementary}

    {html_traditional}
  </script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/js/bootstrap.bundle.min.js" integrity="sha384-FKyoEForCGlyvwx9Hj09JcYn3nv7wiPVlz7YYwJrWVcXK/BmnVDxM+D2scQbITxI" crossorigin="anonymous"></script>
  <script>
    document.addEventListener('shown.bs.tab', function (event) {{
      const filterDiv = document.getElementById("filters");
      if(event.target.id === "pills-about-tab") {{
        filterDiv.classList.add("d-none");
      }} else {{
        filterDiv.classList.remove("d-none");
      }}
    }});
  </script>
  <script>
    function sendHeight() {{
      const height = document.body.scrollHeight || document.documentElement.scrollHeight;
      parent.postMessage({{ type: "resize", height }}, "*");
    }}

    window.addEventListener("load", sendHeight);
    window.addEventListener("resize", sendHeight);
    new MutationObserver(sendHeight).observe(document.body, {{ childList: true, subtree: true }});
  </script>
</body>
</html>
"""

YEAR_FROM = 1950


def generate_html_reports(ti):
    logger = logging.getLogger(__name__)

    language_data = ti.xcom_pull(task_ids='generate_html_language')
    timeline_data = ti.xcom_pull(task_ids='generate_html_timeline')
    map_data = ti.xcom_pull(task_ids='generate_html_map')
    indicators_data = ti.xcom_pull(task_ids='generate_html_indicators')
    journal_data = ti.xcom_pull(task_ids='generate_html_journal')
    doctype_data = ti.xcom_pull(task_ids='generate_html_doctype')
    studytype_data = ti.xcom_pull(task_ids='generate_html_studytype')
    subject_data = ti.xcom_pull(task_ids='generate_html_subject')
    region_data = ti.xcom_pull(task_ids='generate_html_region')
    dimention_data = ti.xcom_pull(task_ids='generate_html_dimention')
    therapy_data = ti.xcom_pull(task_ids='generate_html_therapy')
    complementary_data = ti.xcom_pull(task_ids='generate_html_complementary')
    traditional_data = ti.xcom_pull(task_ids='generate_html_traditional')

    current_date = datetime.now()
    us_date_format = current_date.strftime('%b %d, %Y')

    html_with_data = HTML_TEMPLATE.format(
        html_language=language_data['html'],
        region_options=language_data['region_options'],
        year_range_min=YEAR_FROM,
        year_range_max=language_data['max_year'],
        html_timeline=timeline_data['html'],
        html_map=map_data['html'],
        html_indicators=indicators_data['html'],
        html_journals=journal_data['html'],
        html_doctype=doctype_data['html'],
        html_studytype=studytype_data['html'],
        html_subject=subject_data['html'],
        html_region=region_data['html'],
        html_dimention=dimention_data['html'],
        html_therapy=therapy_data['html'],
        html_complementary=complementary_data['html'],
        html_traditional=traditional_data['html'],
        today=us_date_format
    )

    fs_hook = FSHook(fs_conn_id='TMGL_REGION_HTML_OUTPUT')
    output_dir = fs_hook.get_path()
    output_file = os.path.join(output_dir, "index.html")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_with_data)

    assets_dir = os.path.join(os.path.dirname(__file__), 'assets')
    for item in os.listdir(assets_dir):
        origin = os.path.join(assets_dir, item)
        if os.path.isfile(origin):
            destination = os.path.join(output_dir, item)
            shutil.copy2(origin, destination)

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
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_timeline_task = PythonOperator(
        task_id='generate_html_timeline',
        python_callable=generate_html_timeline,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_map_task = PythonOperator(
        task_id='generate_html_map',
        python_callable=generate_html_map,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_indicators_task = PythonOperator(
        task_id='generate_html_indicators',
        python_callable=generate_html_indicators,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_journal_task = PythonOperator(
        task_id='generate_html_journal',
        python_callable=generate_html_journal,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_doctype_task = PythonOperator(
        task_id='generate_html_doctype',
        python_callable=generate_html_doctype,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_studytype_task = PythonOperator(
        task_id='generate_html_studytype',
        python_callable=generate_html_studytype,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_subject_task = PythonOperator(
        task_id='generate_html_subject',
        python_callable=generate_html_subject,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_region_task = PythonOperator(
        task_id='generate_html_region',
        python_callable=generate_html_region,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_dimention_task = PythonOperator(
        task_id='generate_html_dimention',
        python_callable=generate_html_dimention,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_therapies_task = PythonOperator(
        task_id='generate_html_therapy',
        python_callable=generate_html_therapy,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_complementary_task = PythonOperator(
        task_id='generate_html_complementary',
        python_callable=generate_html_complementary,
        op_kwargs={'year_from': YEAR_FROM},
    )
    generate_html_traditional_task = PythonOperator(
        task_id='generate_html_traditional',
        python_callable=generate_html_traditional,
        op_kwargs={'year_from': YEAR_FROM},
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
    generate_html_doctype_task >> generate_reports_task
    generate_html_studytype_task >> generate_reports_task
    generate_html_subject_task >> generate_reports_task
    generate_html_region_task >> generate_reports_task
    generate_html_dimention_task >> generate_reports_task
    generate_html_therapies_task >> generate_reports_task
    generate_html_complementary_task >> generate_reports_task
    generate_html_traditional_task >> generate_reports_task