local params = import "params.libsonnet";

local airflowCfg = { 
  sections:{ 
    core:{ 
      dags_folder:"/usr/local/airflow/dags",
      base_log_folder:"/usr/local/airflow/logs",
      remote_logging:true,
      remote_log_conn_id:"s3_logs",
      #remote_base_log_folder:"s3://",
      encrypt_s3_logs:true,
      logging_level:"INFO",
      fab_logging_level:"WARN",
      #logging_config_class:"airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG",
      colored_console_log:true,
      colored_log_format:"[%%(blue)s%%(asctime)s%%(reset)s] {{%%(blue)s%%(filename)s:%%(reset)s%%(lineno)d}} %%(log_color)s%%(levelname)s%%(reset)s - %%(log_color)s%%(message)s%%(reset)s",
      colored_formatter_class:"airflow.utils.log.colored_log.CustomTTYColoredFormatter",
      log_format:"[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s",
      simple_log_format:"%%(asctime)s %%(levelname)s - %%(message)s",
      #task_log_prefix_template:null,
      log_filename_template:"{{ti.dag_id}}/{{ti.task_id}}/{{ts}}/{{try_number}}.log",
      log_processor_filename_template:"{{filename}}.log",
      dag_processor_manager_log_location:"/usr/local/airflow/logs/dag_processor_manager/dag_processor_manager.log",
      hostname_callable:"socket:getfqdn",
      default_timezone:"utc",
      executor:"KubernetesExecutor",
      store_serialized_dags:true,
      min_serialized_dag_update_interval: 30,
      #sql_alchemy_conn:"sqlite:////usr/local/airflow/airflow.db",
      sql_engine_encoding:"utf-8",
      sql_alchemy_pool_enabled:true,
      sql_alchemy_pool_size:5,
      sql_alchemy_max_overflow:10,
      sql_alchemy_pool_recycle:1800,
      sql_alchemy_pool_pre_ping:true,
      #sql_alchemy_schema:null,
      parallelism:32,
      dag_concurrency:16,
      dags_are_paused_at_creation:true,
      max_active_runs_per_dag:16,
      load_examples:false,
      load_default_connections:false,
      plugins_folder:"/usr/local/airflow/plugins",
      fernet_key:"{FERNET_KEY}",
      donot_pickle:true,
      dagbag_import_timeout:30,
      task_runner:"StandardTaskRunner",
      #default_impersonation:null,
      #security:null,
      secure_mode:true,
      unit_test_mode:false,
      task_log_reader:"task",
      enable_xcom_pickling:false,
      killed_task_cleanup_time:60,
      dag_run_conf_overrides_params:false,
      worker_precheck:false,
      dag_discovery_safe_mode:true,
      default_task_retries:0,
    },
    cli:{ 
      api_client:"airflow.api.client.local_client",
      endpoint_url:"http://localhost:8080",

    },
    api:{ 
      auth_backend:"airflow.contrib.auth.backends.password_auth",
      authenticate:true,
    },
    lineage:{ 
      backend:null,
    },
    atlas:{ 
      sasl_enabled:false,
      host:null,
      port:21000,
      username:null,
      password:null,

    },
    operators:{ 
      default_owner:"airflow",
      default_cpus:1,
      default_ram:1024,
      default_disk:512,
      default_gpus:0,

    },
    hive:{ 
      #default_hive_mapred_queue:null,
      mapred_job_name_template:"Airflow HiveOperator task for {{hostname}}.{{dag_id}}.{{task_id}}.{{execution_date}}",

    },
    webserver:{ 
      base_url:"https://airflow.tcdata.co/" + params.env,
      web_server_host:"0.0.0.0",
      web_server_port:8080,
      #web_server_ssl_cert:null,
      #web_server_ssl_key:null,
      web_server_master_timeout:120,
      web_server_worker_timeout:120,
      worker_refresh_batch_size:1,
      worker_refresh_interval:30,
      #secret_key:"{SECRET_KEY}",
      workers:4,
      worker_class:"sync",
      access_logfile:"-",
      error_logfile:"-",
      expose_config:false,
      dag_default_view:"tree",
      dag_orientation:"LR",
      demo_mode:false,
      log_fetch_timeout_sec:5,
      hide_paused_dags_by_default:false,
      page_size:100,
      navbar_color:"#3F0E40",
      default_dag_run_display_number:25,
      enable_proxy_fix:true,
      cookie_secure:true,
      #cookie_samesite:null,
      default_wrap:true,
      rbac:true,
    },
    email:{ 
      email_backend:"airflow.utils.email.send_email_smtp"
    },
    smtp:{ 
      smtp_host:"email-smtp.us-east-1.amazonaws.com",
      smtp_starttls:true,
      smtp_ssl:false,
      smtp_port:587,
      smtp_mail_from:"airflow@tcdata.co",
      #smtp_user:null,
      #smtp_password:null,

    },
    sentry:{ 
      sentry_dsn:null,

    },
    celery:{ 
      celery_app_name:"airflow.executors.celery_executor",
      worker_concurrency:16,
      worker_log_server_port:8793,
      #broker_url:null,
      #result_backend:null,
      flower_host:"0.0.0.0",
      #flower_url_prefix:null,
      flower_port:5555,
      #flower_basic_auth:null,
      default_queue:"default",
      sync_parallelism:0,
      celery_config_options:"airflow.config_templates.default_celery.DEFAULT_CELERY_CONFIG",
      ssl_active:false,
      #ssl_key:null,
      #ssl_cert:null,
      #ssl_cacert:null,
      pool:"prefork",

    },
    celery_broker_transport_options:{ 

    },
    dask:{ 
      cluster_address:"127.0.0.1:8786",
      #tls_ca:null,
      #tls_cert:null,
      #tls_key:null
    },
    scheduler:{ 
      job_heartbeat_sec:5,
      scheduler_heartbeat_sec:30,
      num_runs:-1,
      processor_poll_interval:1,
      min_file_process_interval:120,
      dag_dir_list_interval:300,
      print_stats_interval:300,
      scheduler_health_check_threshold:120,
      child_process_log_directory:"/usr/local/airflow/logs/scheduler",
      scheduler_zombie_task_threshold:300,
      catchup_by_default:false,
      max_tis_per_query:1024,
      statsd_on:false,
      statsd_host:"localhost",
      statsd_port:8125,
      statsd_prefix:"airflow",
      statsd_allow_list:null,
      max_threads:8,
      authenticate:false,
      use_job_schedule:true,
    },
    ldap:{ 
      #uri:null,
      user_filter:"objectClass=*",
      user_name_attr:"uid",
      group_member_attr:"memberOf",
      #superuser_filter:null,
      #data_profiler_filter:null,
      bind_user:"cn=Manager,dc=example,dc=com",
      bind_password:"insecure",
      basedn:"dc=example,dc=com",
      cacert:"/etc/ca/ldap_ca.crt",
      search_scope:"LEVEL",
      ignore_malformed_schema:false,

    },
    kerberos:{ 
      ccache:"/tmp/airflow_krb5_ccache",
      principal:"airflow",
      reinit_frequency:3600,
      kinit_path:"kinit",
      keytab:"airflow.keytab",

    },
    github_enterprise:{ 
      api_rev:"v3",

    },
    admin:{ 
      hide_sensitive_variable_fields:true,

    },
    elasticsearch:{ 
      #host:null,
      log_id_template:"{{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}",
      end_of_log_mark:"end_of_log",
      #frontend:null,
      write_stdout:false,
      json_format:false,
      json_fields:"asctime, filename, lineno, levelname, message",

    },
    elasticsearch_configs:{ 
      use_ssl:false,
      verify_certs:true,

    },
    kubernetes:{ 
      worker_container_repository:params.image.repo,
      worker_container_tag:params.image.tag,
      worker_container_image_pull_policy:"IfNotPresent",
      delete_worker_pods:true,
      worker_pods_creation_batch_size:1,
      namespace:"airflow",
      airflow_configmap:"airflow" + "-" + params.env,
      dags_in_image:true,
      #dags_volume_subpath:null,
      #dags_volume_claim:null,
      #logs_volume_subpath:null,
      #logs_volume_claim:null,
      #dags_volume_host:null,
      #env_from_configmap_ref:params.app + "-" + params.env + "-" + "env",
      env_from_secret_ref:params.app + "-" + params.env + "," + params.app + "-" + params.env + "-" + "postgres-auth" + "," + params.app + "-" + params.env + "-" + "s3",
      #git_repo:null,
      #git_branch:null,
      #git_subpath:null,
      #git_user:null,
      #git_password:null,
      git_sync_root:"/git",
      git_sync_dest:"repo",
      #git_dags_folder_mount_point:null,
      #git_ssh_key_secret_name:null,
      #git_ssh_known_hosts_configmap_name:null,
      #git_sync_credentials_secret:null,
      git_sync_container_repository:"k8s.gcr.io/git-sync",
      git_sync_container_tag:"v3.1.1",
      git_sync_init_container_name:"git-sync-clone",
      git_sync_run_as_user:65533,
      #worker_service_account_name:null,
      #image_pull_secrets:null,
      #gcp_service_account_keys:null,
      in_cluster:true,
      #affinity:null,
      #tolerations:null,
      #kube_client_request_args:null,
      run_as_user:1000,
      fs_group:1000,
      #worker_annotations:null,

    },
    kubernetes_node_selectors:{ 

    },
    kubernetes_environment_variables:{
      AWS_ACCESS_KEY_ID: "$(S3_AWS_ACCESS_KEY_ID)",
      AWS_SECRET_ACCESS_KEY: "$(S3_AWS_SECRET_ACCESS_KEY)",
      AIRFLOW_CONN_S3_LOGS: "s3://$(BUCKET_NAME)",
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: "postgresql+psycopg2://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@" + params.app + "-" + params.env + "-" + "postgres" + "/postgres",
      AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER: "s3://$(BUCKET_NAME)/logs",
    },
    kubernetes_secrets:{ 

    },
    kubernetes_labels:{ 

    },

  },
};

{
  apiVersion: "v1",
  kind: "ConfigMap",
  metadata: {
    name: params.app + "-" + params.env,
    labels: {
      app: params.app,
      env: params.env,
    },
    namespace: "airflow",
  },
  data: {
    "airflow.cfg": std.manifestIni(airflowCfg),
    "webserver_config.py": importstr "webserver/webserver_config.py",
  },
}
