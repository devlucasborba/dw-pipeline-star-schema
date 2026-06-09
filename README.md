# Bella Aroma — Data Warehouse com Star Schema

## O Negócio

A **Bella Aroma** é uma rede fictícia de cafeterias com operação exclusivamente via **delivery**. Possui múltiplas lojas físicas distribuídas em diferentes cidades, mas essas lojas não realizam atendimento presencial — sua função é servir como **ponto de origem dos pedidos**, definindo a região de cobertura de cada entrega.

O cardápio é composto por três categorias de produtos: **bebidas quentes**, **bebidas frias** e **snacks**. O estoque é **centralizado e único** — todas as lojas consomem do mesmo inventário, controlado diretamente no cadastro de cada produto.

| Aspecto | Decisão |
|---|---|
| Canal de venda | Somente delivery |
| Atendimento presencial | Não há |
| Função das lojas | Origem geográfica do pedido / região de cobertura |
| Estoque | Centralizado — controlado no produto |
| Responsável pela entrega | Entregador vinculado à loja |

---

## Arquitetura

```
OLTP (PostgreSQL)  ──ETL (Apache Airflow)──►  DW (PostgreSQL)  ──►  Dashboard (Power BI)
```

### Stack

| Camada | Tecnologia |
|---|---|
| Banco transacional (OLTP) | PostgreSQL 16 |
| Banco analítico (OLAP) | PostgreSQL 16 |
| Orquestração do ETL | Apache Airflow 3 |
| Infraestrutura | Docker / Docker Compose |
| Dashboard | Power BI Desktop |

---

## Modelagem OLTP

Banco normalizado (3FN) que serve como fonte de dados para o DW.

| Tabela | Descrição |
|---|---|
| `CATEGORIAS` | Lookup de categorias de produto |
| `CLIENTES` | Cadastro de clientes |
| `LOJAS` | Cadastro de lojas (origem dos pedidos) |
| `ENTREGADORES` | Cadastro de entregadores, vinculados a uma loja |
| `PRODUTOS` | Cadastro de produtos com estoque e preço |
| `PEDIDOS` | Cabeçalho do pedido (loja, entregador, cliente, datas) |
| `ITENS_PEDIDO` | Linhas do pedido (produto, quantidade, valor, desconto) |

---

## Modelagem DW — Star Schema

![Star Schema](https://github.com/devlucasborba/dw-pipeline-star-schema/blob/master/img/starschema.png)

### Fact Table

| Tabela | Grão |
|---|---|
| `fact_venda` | 1 linha por item de pedido |

| Métrica | Cálculo |
|---|---|
| `valor_receita_bruta` | `quantidade × valor_unitario` |
| `valor_desconto` | Desconto aplicado ao item |
| `valor_receita_liq` | `valor_receita_bruta - valor_desconto` |
| `valor_custo` | `quantidade × custo_unitario` |
| `qtd_itens` | Quantidade de unidades do item |
| `tempo_entrega_min` | Diferença em minutos entre `dt_pedido` e `dt_entrega` |

### Dimensões

| Dimensão | Origem no OLTP |
|---|---|
| `dim_tempo` | Gerada a partir de `pedidos.dt_pedido` — grão hora |
| `dim_produto` | `PRODUTOS` + `CATEGORIAS` (join desnormalizado) |
| `dim_cliente` | `CLIENTES` |
| `dim_loja` | `LOJAS` |
| `dim_entregador` | `ENTREGADORES` + `LOJAS` (nome da loja desnormalizado) |

---

## ETL — Apache Airflow

![Pipeline](https://github.com/devlucasborba/dw-pipeline-star-schema/blob/master/img/pipeline.png)

DAG `bella_aroma_etl` com carga incremental diária (`schedule: 0 3 * * *`).

### Fluxo de execução

```
dim_produto  ─┐
dim_cliente  ─┤
dim_loja     ─┼──► dim_tempo ──► fact_venda
dim_entregador┘
```

As dimensões estáticas rodam em paralelo uma única vez por execução. A `dim_tempo` e a `fact_venda` processam apenas os registros do dia de execução.

### Regras de negócio

- Somente pedidos com `status = 1` (entregue) são carregados na `fact_venda`
- Pedidos cancelados (`status = 2`) são excluídos da carga
- `tempo_entrega_min` é calculado no ETL: `(dt_entrega - dt_pedido)` em minutos
- O estoque reflete o valor vigente no momento da carga (SCD Tipo 0)
- Clientes com `status = false` permanecem na dimensão para preservar histórico de vendas
- Deduplicação via `ON CONFLICT DO NOTHING` em todas as tabelas

---

## Dashboard — Power BI

### Visão Geral

![Home](https://github.com/devlucasborba/dw-pipeline-star-schema/blob/master/img/home.png)

KPIs: receita líquida, margem %, total de itens, tempo médio de entrega e desconto total. Análise de receita por mês, por hora do dia, por loja e por categoria de produto.

### Produtos

![Produtos](https://github.com/devlucasborba/dw-pipeline-star-schema/blob/master/img/produtos.png)

Top 10 produtos por receita líquida, receita vs custo vs margem por categoria e tabela de controle de estoque com alerta de estoque crítico.

### Entregas

![Entregadores](https://github.com/devlucasborba/dw-pipeline-star-schema/blob/master/img/entregadores.png)

Tempo médio de entrega por loja com formatação condicional, top 5 entregadores mais rápidos vs mais lentos e gráfico de dispersão volume × tempo por entregador.

### Clientes

![Clientes](https://github.com/devlucasborba/dw-pipeline-star-schema/blob/master/img/clientes.png)

Total de clientes, ativos e em churn. Top 10 clientes por LTV, receita por UF e tabela de clientes em churn com data de saída.

---

## Como executar

### Pré-requisitos

- Docker e Docker Compose instalados
- Power BI Desktop instalado

### Subir o ambiente

```bash
docker compose up -d
```

### Popular o OLTP

```bash
psql -U lucas -d oltp_db -f bella_aroma_oltp_seed.sql
```

### Configurar as connections no Airflow

Acessa `localhost:8080` → **Admin → Connections** e cria:

| Conn Id | Host | Database | Login | Port |
|---|---|---|---|---|
| `postgres_oltp` | `datawarehouse-oltp_db-1` | `oltp_db` | `lucas` | `5432` |
| `postgres_olap` | `datawarehouse-olap_db-1` | `olap_db` | `lucas` | `5433` |

### Ativar a DAG e rodar o backfill

```bash
docker exec -it datawarehouse-airflow-scheduler-1 airflow dags unpause bella_aroma_etl

docker exec -it datawarehouse-airflow-scheduler-1 airflow backfill create \
  --dag-id bella_aroma_etl \
  --from-date 2022-03-01 \
  --to-date 2024-03-31
```

### Conectar o Power BI

Obter Dados → PostgreSQL → `localhost:5433` → `olap_db`
