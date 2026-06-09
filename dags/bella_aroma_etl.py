from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# Configuração da DAG
# ─────────────────────────────────────────────
default_args = {
    "owner": "bella_aroma",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="bella_aroma_etl",
    description="ETL incremental OLTP → OLAP — Bella Aroma",
    default_args=default_args,
    start_date=datetime(2022, 3, 1),
    schedule="0 3 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["bella_aroma", "etl", "dimensional"],
)


# ─────────────────────────────────────────────
# TASK 1 — dim_produto
# Roda uma única vez: insere apenas produtos
# ainda não presentes no OLAP.
# ON CONFLICT garante idempotência.
# ─────────────────────────────────────────────
def load_dim_produto(**context):
    oltp_hook = PostgresHook(postgres_conn_id="postgres_oltp")
    olap_hook = PostgresHook(postgres_conn_id="postgres_olap")

    olap_ids = {
        row[0]
        for row in olap_hook.get_records("SELECT id_produto FROM dim_produto")
    }

    rows = oltp_hook.get_records(
        """
        SELECT
            p.id,
            p.nome,
            c.nome           AS nm_categoria,
            p.preco_tabela,
            p.custo_unitario,
            p.estoque_atual,
            p.estoque_minimo,
            p.ativo
        FROM PRODUTOS p
        JOIN CATEGORIAS c ON c.id = p.categoria_id
        """
    )

    novos = [r for r in rows if r[0] not in olap_ids]

    if not novos:
        print("[dim_produto] Nenhum produto novo.")
        return

    olap_conn   = olap_hook.get_conn()
    olap_cursor = olap_conn.cursor()

    olap_cursor.executemany(
        """
        INSERT INTO dim_produto (
            id_produto, nm_produto, nm_categoria,
            preco_tabela, custo_unitario,
            estoque_atual, estoque_minimo, ativo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_produto) DO NOTHING
        """,
        novos
    )

    olap_conn.commit()
    print(f"[dim_produto] {olap_cursor.rowcount} produtos inseridos.")
    olap_cursor.close()
    olap_conn.close()


# ─────────────────────────────────────────────
# TASK 2 — dim_cliente
# ─────────────────────────────────────────────
def load_dim_cliente(**context):
    oltp_hook = PostgresHook(postgres_conn_id="postgres_oltp")
    olap_hook = PostgresHook(postgres_conn_id="postgres_olap")

    olap_ids = {
        row[0]
        for row in olap_hook.get_records("SELECT id_cliente FROM dim_cliente")
    }

    rows = oltp_hook.get_records(
        """
        SELECT
            id,
            nome,
            cidade,
            uf,
            DATE(created_at),
            status,
            DATE(data_churn)
        FROM CLIENTES
        """
    )

    novos = [r for r in rows if r[0] not in olap_ids]

    if not novos:
        print("[dim_cliente] Nenhum cliente novo.")
        return

    olap_conn   = olap_hook.get_conn()
    olap_cursor = olap_conn.cursor()

    olap_cursor.executemany(
        """
        INSERT INTO dim_cliente (
            id_cliente, nm_cliente, cidade, uf,
            dt_cadastro, status, data_churn
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_cliente) DO NOTHING
        """,
        novos
    )

    olap_conn.commit()
    print(f"[dim_cliente] {olap_cursor.rowcount} clientes inseridos.")
    olap_cursor.close()
    olap_conn.close()


# ─────────────────────────────────────────────
# TASK 3 — dim_loja
# ─────────────────────────────────────────────
def load_dim_loja(**context):
    oltp_hook = PostgresHook(postgres_conn_id="postgres_oltp")
    olap_hook = PostgresHook(postgres_conn_id="postgres_olap")

    olap_ids = {
        row[0]
        for row in olap_hook.get_records("SELECT id_loja FROM dim_loja")
    }

    rows  = oltp_hook.get_records("SELECT id, nome, cidade, uf FROM LOJAS")
    novos = [r for r in rows if r[0] not in olap_ids]

    if not novos:
        print("[dim_loja] Nenhuma loja nova.")
        return

    olap_conn   = olap_hook.get_conn()
    olap_cursor = olap_conn.cursor()

    olap_cursor.executemany(
        """
        INSERT INTO dim_loja (id_loja, nm_loja, cidade, uf)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id_loja) DO NOTHING
        """,
        novos
    )

    olap_conn.commit()
    print(f"[dim_loja] {olap_cursor.rowcount} lojas inseridas.")
    olap_cursor.close()
    olap_conn.close()


# ─────────────────────────────────────────────
# TASK 4 — dim_entregador
# ─────────────────────────────────────────────
def load_dim_entregador(**context):
    oltp_hook = PostgresHook(postgres_conn_id="postgres_oltp")
    olap_hook = PostgresHook(postgres_conn_id="postgres_olap")

    olap_ids = {
        row[0]
        for row in olap_hook.get_records("SELECT id_entregador FROM dim_entregador")
    }

    rows = oltp_hook.get_records(
        """
        SELECT
            e.id,
            e.nome,
            l.nome         AS nm_loja_base,
            DATE(e.dt_admissao),
            e.ativo
        FROM ENTREGADORES e
        JOIN LOJAS l ON l.id = e.loja_id
        """
    )

    novos = [r for r in rows if r[0] not in olap_ids]

    if not novos:
        print("[dim_entregador] Nenhum entregador novo.")
        return

    olap_conn   = olap_hook.get_conn()
    olap_cursor = olap_conn.cursor()

    olap_cursor.executemany(
        """
        INSERT INTO dim_entregador (
            id_entregador, nm_entregador,
            nm_loja_base, dt_admissao, ativo
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id_entregador) DO NOTHING
        """,
        novos
    )

    olap_conn.commit()
    print(f"[dim_entregador] {olap_cursor.rowcount} entregadores inseridos.")
    olap_cursor.close()
    olap_conn.close()


# ─────────────────────────────────────────────
# TASK 5 — dim_tempo
# Grão: 1 linha por (data + hora) com pedidos no dia
# ON CONFLICT garante idempotência sem loop
# ─────────────────────────────────────────────
def load_dim_tempo(**context):
    ds        = context["ds"]
    oltp_hook = PostgresHook(postgres_conn_id="postgres_oltp")
    olap_hook = PostgresHook(postgres_conn_id="postgres_olap")

    rows = oltp_hook.get_records(
        """
        SELECT DISTINCT
            DATE(dt_pedido)                           AS dt_referencia,
            EXTRACT(HOUR FROM dt_pedido)::SMALLINT    AS hora,
            EXTRACT(YEAR FROM dt_pedido)::SMALLINT    AS ano,
            EXTRACT(QUARTER FROM dt_pedido)::SMALLINT AS trimestre,
            EXTRACT(MONTH FROM dt_pedido)::SMALLINT   AS mes,
            TO_CHAR(dt_pedido, 'TMMonth')             AS nm_mes,
            EXTRACT(WEEK FROM dt_pedido)::SMALLINT    AS semana_ano,
            EXTRACT(DAY FROM dt_pedido)::SMALLINT     AS dia_mes,
            TO_CHAR(dt_pedido, 'TMDay')               AS nm_dia_semana,
            EXTRACT(DOW FROM dt_pedido) IN (0, 6)     AS fim_de_semana
        FROM PEDIDOS
        WHERE DATE(dt_pedido) = %(ds)s
        """,
        parameters={"ds": ds},
    )

    if not rows:
        print(f"[dim_tempo] Nenhum registro para {ds}.")
        return

    olap_conn   = olap_hook.get_conn()
    olap_cursor = olap_conn.cursor()

    olap_cursor.executemany(
        """
        INSERT INTO dim_tempo (
            dt_referencia, hora, ano, trimestre, mes,
            nm_mes, semana_ano, dia_mes, nm_dia_semana, fim_de_semana
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dt_referencia, hora) DO NOTHING
        """,
        rows
    )

    olap_conn.commit()
    print(f"[dim_tempo] {olap_cursor.rowcount} registros inseridos para {ds}.")
    olap_cursor.close()
    olap_conn.close()


# ─────────────────────────────────────────────
# TASK 6 — fact_venda
# Somente pedidos com status = 1 (entregue)
# Somente itens cujo dt_pedido = dia de execução
# ON CONFLICT garante idempotência
# ─────────────────────────────────────────────
def load_fact_venda(**context):
    ds        = context["ds"]
    oltp_hook = PostgresHook(postgres_conn_id="postgres_oltp")
    olap_hook = PostgresHook(postgres_conn_id="postgres_olap")

    rows = oltp_hook.get_records(
        """
        SELECT
            ip.id                                              AS id_item,
            ip.produto_id,
            p.cliente_id,
            p.loja_id,
            p.entregador_id,
            DATE(p.dt_pedido)                                  AS dt_ref,
            EXTRACT(HOUR FROM p.dt_pedido)::INT                AS hora,
            ip.quantidade,
            ROUND(ip.quantidade * ip.valor_unitario, 2)        AS vlr_receita_bruta,
            COALESCE(ip.valor_desconto, 0)                     AS vlr_desconto,
            ROUND(
                ip.quantidade * ip.valor_unitario
                - COALESCE(ip.valor_desconto, 0), 2
            )                                                  AS vlr_receita_liq,
            ROUND(ip.quantidade * pr.custo_unitario, 2)        AS vlr_custo,
            ROUND(
                EXTRACT(EPOCH FROM (p.dt_entrega - p.dt_pedido)) / 60
            )::INT                                             AS tempo_entrega_min
        FROM ITENS_PEDIDO ip
        JOIN PEDIDOS  p  ON p.id  = ip.pedido_id
        JOIN PRODUTOS pr ON pr.id = ip.produto_id
        WHERE p.status          = 1
          AND DATE(p.dt_pedido) = %(ds)s
        """,
        parameters={"ds": ds},
    )

    if not rows:
        print(f"[fact_venda] Nenhum item para {ds}.")
        return

    # Carrega dicionários de surrogate keys do OLAP
    sk_tempo      = {(r[0], r[1]): r[2] for r in olap_hook.get_records(
        "SELECT dt_referencia, hora, sk_tempo FROM dim_tempo")}
    sk_produto    = {r[0]: r[1] for r in olap_hook.get_records(
        "SELECT id_produto, sk_produto FROM dim_produto")}
    sk_cliente    = {r[0]: r[1] for r in olap_hook.get_records(
        "SELECT id_cliente, sk_cliente FROM dim_cliente")}
    sk_loja       = {r[0]: r[1] for r in olap_hook.get_records(
        "SELECT id_loja, sk_loja FROM dim_loja")}
    sk_entregador = {r[0]: r[1] for r in olap_hook.get_records(
        "SELECT id_entregador, sk_entregador FROM dim_entregador")}

    olap_conn   = olap_hook.get_conn()
    olap_cursor = olap_conn.cursor()
    inserted    = 0
    skipped     = 0

    for row in rows:
        (id_item, produto_id, cliente_id, loja_id, entregador_id,
         dt_ref, hora, qtd, rec_bruta, desconto, rec_liq, custo, tempo_min) = row

        sk_t = sk_tempo.get((dt_ref, int(hora)))
        sk_p = sk_produto.get(produto_id)
        sk_c = sk_cliente.get(cliente_id)
        sk_l = sk_loja.get(loja_id)
        sk_e = sk_entregador.get(entregador_id)

        if None in (sk_t, sk_p, sk_c, sk_l, sk_e):
            print(f"[fact_venda] AVISO: SK ausente para item {id_item} "
                  f"(t={sk_t} p={sk_p} c={sk_c} l={sk_l} e={sk_e}) — pulando.")
            skipped += 1
            continue

        olap_cursor.execute(
            """
            INSERT INTO fact_venda (
                sk_tempo, sk_produto, sk_cliente, sk_loja, sk_entregador,
                qtd_itens, valor_receita_bruta, valor_desconto,
                valor_receita_liq, valor_custo, tempo_entrega_min
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sk_tempo, sk_produto, sk_cliente, sk_loja, sk_entregador)
            DO NOTHING
            """,
            (sk_t, sk_p, sk_c, sk_l, sk_e,
             qtd, rec_bruta, desconto, rec_liq, custo, tempo_min)
        )
        inserted += olap_cursor.rowcount

    olap_conn.commit()
    olap_cursor.close()
    olap_conn.close()
    print(f"[fact_venda] {inserted} inseridos, {skipped} pulados para {ds}.")


# ─────────────────────────────────────────────
# Declaração das tasks e dependências
#
# Fluxo:
#   dim_produto  ─┐
#   dim_cliente  ─┤
#   dim_loja     ─┼──► dim_tempo ──► fact_venda
#   dim_entregador┘
#
# Dimensões estáticas rodam primeiro e uma única
# vez por execução. dim_tempo depende delas pois
# a fact depende de todas.
# ─────────────────────────────────────────────
with dag:
    t_dim_produto = PythonOperator(
        task_id="load_dim_produto",
        python_callable=load_dim_produto,
    )
    t_dim_cliente = PythonOperator(
        task_id="load_dim_cliente",
        python_callable=load_dim_cliente,
    )
    t_dim_loja = PythonOperator(
        task_id="load_dim_loja",
        python_callable=load_dim_loja,
    )
    t_dim_entregador = PythonOperator(
        task_id="load_dim_entregador",
        python_callable=load_dim_entregador,
    )
    t_dim_tempo = PythonOperator(
        task_id="load_dim_tempo",
        python_callable=load_dim_tempo,
    )
    t_fact_venda = PythonOperator(
        task_id="load_fact_venda",
        python_callable=load_fact_venda,
    )

    # Dimensões estáticas em paralelo → dim_tempo → fact_venda
    [t_dim_produto, t_dim_cliente, t_dim_loja, t_dim_entregador] >> t_dim_tempo >> t_fact_venda
