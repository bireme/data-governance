import os
import logging
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_regions.misc import get_country_data
from data_governance.dags.tmgl_countries.misc import get_eligible_countries
from data_governance.dags.tmgl_countries.tasks_for_export.indicator import generate_html_indicators
from data_governance.dags.tmgl_countries.tasks_for_export.doctype import generate_html_doctype
from data_governance.dags.tmgl_countries.tasks_for_export.studytype import generate_html_studytype
from data_governance.dags.tmgl_countries.tasks_for_export.subject import generate_html_subject
from data_governance.dags.tmgl_countries.tasks_for_export.dimention import generate_html_dimention
from data_governance.dags.tmgl_countries.tasks_for_export.region import generate_html_region
from data_governance.dags.tmgl_countries.tasks_for_export.therapies import generate_html_therapy
#from data_governance.dags.tmgl_countries.tasks_for_export.complementary import generate_html_complementary
#from data_governance.dags.tmgl_countries.tasks_for_export.traditional import generate_html_traditional


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
  <script src="./wordcloud.js"></script>
  <script src="./drilldown.js"></script>
  <script src="./treemap.js"></script>
  <script src="./highcharts-more.js"></script>
  <script src="./dumbbell.js"></script>
  <script src="./lollipop.js"></script>
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
      <button class="nav-link active" id="pills-output-tab" data-bs-toggle="pill" data-bs-target="#output-tab-pane" type="button" role="tab" aria-controls="output-tab-pane" aria-selected="true">Scientific Country Production</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-topics-countries-tab" data-bs-toggle="pill" data-bs-target="#topics-countries-tab-pane" type="button" role="tab" aria-controls="topics-countries-tab-pane" aria-selected="false">Topics</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-tcim-areas-tab" data-bs-toggle="pill" data-bs-target="#tcim-areas-tab-pane" type="button" role="tab" aria-controls="tcim-areas-tab-pane" aria-selected="false">Main subjects and TCIM areas</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="pills-about-tab" data-bs-toggle="pill" data-bs-target="#about-tab-pane" type="button" role="tab" aria-controls="about-tab-pane" aria-selected="false">About</button>
    </li>
  </ul>

  <div class="d-flex justify-content-center" id="filters">
    <div class="slider-control pt-1 ms-2">
      <div id="yearRangeSlider"></div>
    </div>
  </div>

  <div class="tab-content">
    <div class="tab-pane fade show active" id="output-tab-pane" role="tabpanel" aria-labelledby="pills-output-tab">
      <div class="row mt-4">
        <div class="col-lg-6 col-xs-12">
          <h3 class="h4">Scientific Output</h3>
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
          <p><strong>TM Research Analytics</strong> (beta) is an interactive dashboard developed by BIREME/PAHO/WHO to support the analysis, visualization, and interpretation of global and WHO regional scientific output in the field of Traditional, Complementary, and Integrative Medicine (TCIM).</p>
          <p>It compiles bibliographic records of scientific literature published by various organizations, collected, indexed, or archived in the Traditional Medicine Global Library (TMGL) mega-database, and makes them accessible to users through its integrated search engine (iAHx).</p>
          <p>The dashboard aims to provide researchers, policymakers, managers, and other stakeholders with a comprehensive understanding of the evolution of TCIM scientific production - its geographical distribution, the prominence of specific therapeutic methods, and the emergence of key thematic areas - thereby fostering evidence-informed decision-making.</p>
          <p>Drawing on bibliometric analysis, the dashboard translates indexed data into meaningful insights. Production indicators such as the total number of documents, availability of full texts, and temporal distribution of publications over the past decade reveal not only the scale but also the growth dynamics of TCIM research. The classification of document and study types, together with the identification of leading journals, highlights the main channels through which this knowledge is disseminated and the research methodologies most frequently adopted.</p>
          <p>At the same time, the dashboard enables exploration of content patterns by analyzing the languages of publication, the most frequent subjects, and the thematic orientation of research across the Traditional Medicine Dimensions inspired by the Gujarat Declaration, as well as by therapeutic methods and TCIM areas. This perspective is further enriched by a geographical lens, allowing comparisons across WHO Regions and individual countries, thereby making visible regional research strengths, collaboration trends, and thematic priorities.</p>
          <p>TM Research Analytics (beta) was conceived as a global resource that also provides regional and country-level filters, allowing users to explore 13 core indicators within a single interactive interface. The dashboard was developed through a process of data extraction, cleaning, and classification combined with interactive visualizations built with Highcharts. Beyond presenting numbers, it seeks to narrate the story of TCIM research - how it is produced, where it is concentrated, and which dimensions of traditional medicine are gaining scientific visibility across the world.</p>
          <p>
            <strong>Legend — Study Types (Groupings)</strong>
            <ul>
              <li><strong>Systematic review</strong> = Systematic review + Systematic review of observational studies</li>
              <li><strong>Other Reviews</strong> = Literature review + Review</li>
              <li><strong>Other studies</strong> = Diagnostic Etiology Prognostic Prevalence Screening Incidence Health technology assessment Health economic evaluation Evaluation study Overview/Evidence synthesis</li>
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
          <p><strong>How to cite: BIREME/PAHO/WHO.</strong> <em>The WHO Traditional Medicine Global Library (TMGL)</em> [Internet]. São Paulo: BIREME/PAHO/WHO; 2025 [cited 2025 Oct 13]. Available from: <a href="https://staging.org">https://staging.org</a></p>
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

    {html_indicators}

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


def copy_assets():
    fs_hook = FSHook(fs_conn_id='TMGL_COUNTRIES_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    assets_dir = os.path.join(os.path.dirname(__file__), 'assets')
    for item in os.listdir(assets_dir):
        origin = os.path.join(assets_dir, item)
        if os.path.isfile(origin):
            destination = os.path.join(output_dir, item)
            shutil.copy2(origin, destination)


def generate_html_reports(country):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    who_region_collection = mongo_hook.get_collection('who_region', 'TABS')
    country_data = get_country_data(who_region_collection, country)
    country_iso = next((iso for iso in country_data.get("pais_sinonimo", []) if len(iso) == 2), None)
    country_iso = country_iso.lower()

    logger.info(f"Gerando HTML - {country} - {country_iso}")

    doctype_data = generate_html_doctype(YEAR_FROM, country, country_iso)
    studytype_data = generate_html_studytype(YEAR_FROM, country, country_iso)
    indicators_data = generate_html_indicators(YEAR_FROM, country, country_iso)
    subject_data = generate_html_subject(YEAR_FROM, country, country_iso)
    dimention_data = generate_html_dimention(YEAR_FROM, country, country_iso)
    region_data = generate_html_region(YEAR_FROM, country, country_iso)
    therapy_data = generate_html_therapy(YEAR_FROM, country, country_iso)
    """complementary_data = ti.xcom_pull(task_ids='generate_html_complementary')
    traditional_data = ti.xcom_pull(task_ids='generate_html_traditional')"""

    html_with_data = HTML_TEMPLATE.format(
        year_range_min=YEAR_FROM,
        year_range_max=doctype_data['max_year'],
        html_doctype=doctype_data['html'],
        html_indicators=indicators_data['html'],
        html_studytype=studytype_data['html'],
        html_subject=subject_data['html'],
        html_dimention=dimention_data['html'],
        html_region=region_data['html'],
        html_therapy=therapy_data['html'],
        html_complementary="",
        html_traditional="",
    )
    """html_complementary=complementary_data['html'],
        html_traditional=traditional_data['html'],"""

    fs_hook = FSHook(fs_conn_id='TMGL_COUNTRIES_HTML_OUTPUT')
    output_dir = fs_hook.get_path()
    output_file = os.path.join(output_dir, country_iso + ".html")

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
    'TMGL_COUNTRIES_03_export_html',
    default_args=default_args,
    description='TMGL - Exporta HTML com métricas de documentos por país',
    schedule_interval=None,
    catchup=False,
    tags=['tmgl', 'report', 'html']
) as dag:
    get_eligible_countries_task = PythonOperator(
        task_id='get_eligible_countries',
        python_callable=get_eligible_countries
    )
    copy_assets_task = PythonOperator(
        task_id='copy_assets',
        python_callable=copy_assets
    )
    generate_reports_task = PythonOperator.partial(
        task_id='generate_html_reports',
        python_callable=generate_html_reports
    ).expand(op_args=get_eligible_countries_task.output)

    copy_assets_task
    get_eligible_countries_task >> generate_reports_task