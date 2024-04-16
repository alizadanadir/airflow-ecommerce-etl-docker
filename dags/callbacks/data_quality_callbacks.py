from airflow.providers.postgres.hooks.postgres import PostgresHook

postgres_conn_id = 'postgresql_de'

def user_activity_log_success(context):
    ti = context['ti']
    execution_date = ti.execution_date

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    insert_query = f"insert into staging.dq_checks_results values('user_activity_log', 'isNull', '{execution_date}', 1)"
    engine.execute(insert_query)

def user_activity_log_failure(context):
    ti = context['ti']
    execution_date = ti.execution_date

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    insert_query = f"insert into staging.dq_checks_results values('user_activity_log', 'isNull', '{execution_date}', 0)"
    engine.execute(insert_query)

def user_order_log_success(context):
    ti = context['ti']
    execution_date = ti.execution_date

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    insert_query = f"insert into staging.dq_checks_results values('user_order_log', 'isNull', '{execution_date}', 1)"
    engine.execute(insert_query)


def user_order_log_failure(context):
    ti = context['ti']
    execution_date = ti.execution_date

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    insert_query = f"insert into staging.dq_checks_results values('user_order_log', 'isNull', '{execution_date}', 0)"
    engine.execute(insert_query)