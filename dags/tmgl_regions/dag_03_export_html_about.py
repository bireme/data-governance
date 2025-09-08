import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="UTF-8" />
  <title>TM Research Analytics</title>

  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-sRIl4kxILFvY47J16cr9ZwB07vP4J8+LH7qKQnuqkuIAvNWLzeN8tE5YBujZqJLB" crossorigin="anonymous">
  <style>@import url('https://fonts.googleapis.com/css2?family=Roboto:ital,wght@0,100..900;1,100..900&display=swap');</style>
  <link href="tmgl_regions.css" rel="stylesheet">
</head>
<body>
  <h2><img src="icone_tmgl.svg"> TM Research Analytics</h2>

  <ul class="nav nav-pills nav-justified custom-nav my-3">
    <li class="nav-item">
      <a class="nav-link" href="index.html">Global Scientific Output</a>
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
      <a class="nav-link selected" href="#">About</a>
    </li>
  </ul>

  <div class="row mt-4">
    <div class="col-xs-12">
      <h3 class="h4">About</h3>
      <p><strong>TM Research Analytics</strong> is an interactive tool designed to support the analysis, visualization, and interpretation of global scientific output related to <strong>Traditional, Complementary, and Integrative Medicine (TCIM)</strong>.</p>
      <p>Its purpose is to provide researchers, policymakers, managers, and other stakeholders with a comprehensive overview of trends, geographical distribution, therapeutic methods, and key thematic areas in this field, facilitating evidence-informed decision-making.</p>
      <p>The dashboard was developed based on data indexed in the <strong>Traditional Medicine Global Library (TMGL)</strong> and related databases. The process involved data extraction, cleaning, and classification, followed by the creation of interactive visualizations using <strong>Python</strong>. The design and indicators were defined collaboratively by BIREME/PAHO/WHO in alignment with the needs of the WHO Traditional Medicine Centre (TMC) and the Department of Digital Health and Innovation (DHI).</p>
    </div>
  </div>
</body>
</html>
"""


def generate_html_about():
    logger = logging.getLogger(__name__)

    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()
    output_file = os.path.join(output_dir, "about.html")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(HTML_TEMPLATE)

    logger.info(f"HTML gerado e salvo em {output_file}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'TMGL_REGION_03_export_html_about',
    default_args=default_args,
    description='TMGL - Exporta HTML com conteúdo About',
    schedule_interval=None,
    catchup=False,
    tags=['tmgl', 'about', 'html']
) as dag:
    generate_html_about_task = PythonOperator(
        task_id='generate_html_about',
        python_callable=generate_html_about
    )