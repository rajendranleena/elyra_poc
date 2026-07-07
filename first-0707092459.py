from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow import DAG

import pendulum

args = {
    "project_id": "first-0707092459",
}

dag = DAG(
    dag_id="first-0707092459",
    default_args=args,
    schedule="@once",
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    description="""
Created with Elyra 4.1.1 pipeline editor using `first.pipeline`.
    """,
    is_paused_upon_creation=False,
)


# Operator source: utils/leena/elyra/01_generate.ipynb

op_0ba5d858_1dd3_4d64_b21c_a1024da15f3f = KubernetesPodOperator(
    name="01_generate",
    namespace="es-tenant-2",
    image="nx1registry-hgb9hgbwd8baguad.azurecr.io/jupyterhub:5.4-nx1.6-elyra-poc1",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p ./jupyter-work-dir/ && cd ./jupyter-work-dir/ && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/elyra/airflow/bootstrapper.py' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/elyra/airflow/bootstrapper.py --output bootstrapper.py && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/etc/generic/requirements-elyra.txt' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/etc/generic/requirements-elyra.txt --output requirements-elyra.txt && python3 -m pip install packaging && python3 -m pip freeze > requirements-current.txt && python3 bootstrapper.py --pipeline-name 'first' --cos-endpoint https://s3.us-east-1.amazonaws.com --cos-bucket nx1poc-pdc-default-ygglold --cos-directory 'first-0707092459' --cos-dependencies-archive '01_generate-0ba5d858-1dd3-4d64-b21c-a1024da15f3f.tar.gz' --file 'utils/leena/elyra/01_generate.ipynb' --outputs 'raw_sales.csv' "
    ],
    task_id="01_generate",
    env_vars={
        "ELYRA_RUNTIME_ENV": "airflow",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
        "ELYRA_RUN_NAME": "first-{{ ts_nodash }}",
    },
    volumes=[],
    volume_mounts=[],
    secrets=[
        Secret("env", "AWS_ACCESS_KEY_ID", "elyra-cos-creds", "AWS_ACCESS_KEY_ID"),
        Secret(
            "env", "AWS_SECRET_ACCESS_KEY", "elyra-cos-creds", "AWS_SECRET_ACCESS_KEY"
        ),
    ],
    annotations={},
    labels={},
    tolerations=[],
    in_cluster=True,
    config_file=None,
    dag=dag,
)

op_0ba5d858_1dd3_4d64_b21c_a1024da15f3f.image_pull_policy = "IfNotPresent"


# Operator source: utils/leena/elyra/02_transform.ipynb

op_bea2611e_291c_4c36_b5a0_7a8cf2d49453 = KubernetesPodOperator(
    name="02_transform",
    namespace="es-tenant-2",
    image="nx1registry-hgb9hgbwd8baguad.azurecr.io/jupyterhub:5.4-nx1.6-elyra-poc1",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p ./jupyter-work-dir/ && cd ./jupyter-work-dir/ && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/elyra/airflow/bootstrapper.py' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/elyra/airflow/bootstrapper.py --output bootstrapper.py && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/etc/generic/requirements-elyra.txt' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/etc/generic/requirements-elyra.txt --output requirements-elyra.txt && python3 -m pip install packaging && python3 -m pip freeze > requirements-current.txt && python3 bootstrapper.py --pipeline-name 'first' --cos-endpoint https://s3.us-east-1.amazonaws.com --cos-bucket nx1poc-pdc-default-ygglold --cos-directory 'first-0707092459' --cos-dependencies-archive '02_transform-bea2611e-291c-4c36-b5a0-7a8cf2d49453.tar.gz' --file 'utils/leena/elyra/02_transform.ipynb' --inputs 'raw_sales.csv' --outputs 'summary_by_region.csv' "
    ],
    task_id="02_transform",
    env_vars={
        "ELYRA_RUNTIME_ENV": "airflow",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
        "ELYRA_RUN_NAME": "first-{{ ts_nodash }}",
    },
    volumes=[],
    volume_mounts=[],
    secrets=[
        Secret("env", "AWS_ACCESS_KEY_ID", "elyra-cos-creds", "AWS_ACCESS_KEY_ID"),
        Secret(
            "env", "AWS_SECRET_ACCESS_KEY", "elyra-cos-creds", "AWS_SECRET_ACCESS_KEY"
        ),
    ],
    annotations={},
    labels={},
    tolerations=[],
    in_cluster=True,
    config_file=None,
    dag=dag,
)

op_bea2611e_291c_4c36_b5a0_7a8cf2d49453.image_pull_policy = "IfNotPresent"

op_bea2611e_291c_4c36_b5a0_7a8cf2d49453 << op_0ba5d858_1dd3_4d64_b21c_a1024da15f3f


# Operator source: utils/leena/elyra/03_report.ipynb

op_06d1bd3e_6545_4955_b41b_8c2433737ecf = KubernetesPodOperator(
    name="03_report",
    namespace="es-tenant-2",
    image="nx1registry-hgb9hgbwd8baguad.azurecr.io/jupyterhub:5.4-nx1.6-elyra-poc1",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p ./jupyter-work-dir/ && cd ./jupyter-work-dir/ && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/elyra/airflow/bootstrapper.py' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/elyra/airflow/bootstrapper.py --output bootstrapper.py && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/etc/generic/requirements-elyra.txt' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v4.1.1/etc/generic/requirements-elyra.txt --output requirements-elyra.txt && python3 -m pip install packaging && python3 -m pip freeze > requirements-current.txt && python3 bootstrapper.py --pipeline-name 'first' --cos-endpoint https://s3.us-east-1.amazonaws.com --cos-bucket nx1poc-pdc-default-ygglold --cos-directory 'first-0707092459' --cos-dependencies-archive '03_report-06d1bd3e-6545-4955-b41b-8c2433737ecf.tar.gz' --file 'utils/leena/elyra/03_report.ipynb' --inputs 'summary_by_region.csv;raw_sales.csv' --outputs 'report.txt' "
    ],
    task_id="03_report",
    env_vars={
        "ELYRA_RUNTIME_ENV": "airflow",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
        "ELYRA_RUN_NAME": "first-{{ ts_nodash }}",
    },
    volumes=[],
    volume_mounts=[],
    secrets=[
        Secret("env", "AWS_ACCESS_KEY_ID", "elyra-cos-creds", "AWS_ACCESS_KEY_ID"),
        Secret(
            "env", "AWS_SECRET_ACCESS_KEY", "elyra-cos-creds", "AWS_SECRET_ACCESS_KEY"
        ),
    ],
    annotations={},
    labels={},
    tolerations=[],
    in_cluster=True,
    config_file=None,
    dag=dag,
)

op_06d1bd3e_6545_4955_b41b_8c2433737ecf.image_pull_policy = "IfNotPresent"

op_06d1bd3e_6545_4955_b41b_8c2433737ecf << op_bea2611e_291c_4c36_b5a0_7a8cf2d49453
