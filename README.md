# Bella Aroma — Projeto de Estudo: Modelagem Dimensional

## O Negócio

A **Bella Aroma** é uma rede fictícia de cafeterias com operação exclusivamente via **delivery**. Possui múltiplas lojas físicas distribuídas em diferentes cidades, mas essas lojas não realizam atendimento presencial — sua função é servir como **ponto de origem dos pedidos**, definindo a região de cobertura de cada entrega.

O cardápio é composto por três categorias de produtos: **bebidas quentes**, **bebidas frias** e **snacks**. O estoque é **centralizado e único** — todas as lojas consomem do mesmo inventário, controlado diretamente no cadastro de cada produto.

Os pedidos são realizados pelos clientes via aplicativo, atribuídos a uma loja de acordo com a localização, e entregues por entregadores vinculados a essa loja.

---

## Operação

| Aspecto | Decisão |
|---|---|
| Canal de venda | Somente delivery |
| Atendimento presencial | Não há |
| Função das lojas | Origem geográfica do pedido / região de cobertura |
| Estoque | Centralizado — controlado no produto |
| Responsável pela entrega | Entregador vinculado à loja |

---

## Perguntas Analíticas (Dashboard)

### Receita e Volume

- Qual o faturamento total por período (dia / semana / mês / ano)?
- Qual o ticket médio por pedido?
- Qual o volume de pedidos por período?

### Produto

- Quais são os 10 produtos mais vendidos em receita e em quantidade?
- Qual a receita por categoria (bebidas quentes, bebidas frias, snacks)?
- Quais produtos estão abaixo do estoque mínimo?

### Cliente

- Qual o valor total gasto por cliente (LTV simples)?
- Quais clientes entraram em churn nos últimos 30 dias?
- Qual a frequência média de compra por cliente?

### Loja e Localização

- Qual loja gera mais receita?
- Como se distribui a receita por cidade e UF?
- Qual o ticket médio por loja?

### Entregador

- Quais entregadores realizaram mais entregas?
- Qual o tempo médio de entrega por entregador?
- Qual o tempo médio de entrega por loja?

### Tempo

- Qual o horário de pico de pedidos?
- Há sazonalidade por dia da semana ou mês?
- Como a receita evolui semana a semana (WoW)?

---

## Arquitetura de Dados

```
OLTP (transacional)  ──ETL──►  DW (dimensional / Star Schema)
```

### OLTP — Tabelas

| Tabela | Descrição |
|---|---|
| `CATEGORIAS` | Lookup de categorias de produto |
| `CLIENTES` | Cadastro de clientes |
| `LOJAS` | Cadastro de lojas (origem dos pedidos) |
| `ENTREGADORES` | Cadastro de entregadores, vinculados a uma loja |
| `PRODUTOS` | Cadastro de produtos com estoque e preço |
| `PEDIDOS` | Cabeçalho do pedido (loja, entregador, cliente, datas) |
| `ITENS_PEDIDO` | Linhas do pedido (produto, quantidade, valor, desconto) |

### DW — Star Schema

**Fact Table**

| Tabela | Grão |
|---|---|
| `fact_vendas` | 1 linha por item de pedido |

**Métricas da fact_vendas**

| Métrica | Descrição |
|---|---|
| `vlr_receita_bruta` | `quantidade × valor_unitario` |
| `vlr_desconto` | Desconto aplicado ao item |
| `vlr_receita_liq` | `vlr_receita_bruta - vlr_desconto` |
| `vlr_custo` | `quantidade × custo_unitario` |
| `qtd_itens` | Quantidade de unidades do item |
| `tempo_entrega_min` | Diferença em minutos entre `dt_pedido` e `dt_entrega` |

**Dimensões**

| Dimensão | Origem no OLTP |
|---|---|
| `dim_tempo` | Gerada via procedure de calendário a partir de `pedidos.dt_pedido` |
| `dim_produto` | `PRODUTOS` + `CATEGORIAS` (join desnormalizado) |
| `dim_cliente` | `CLIENTES` |
| `dim_loja` | `LOJAS` |
| `dim_entregador` | `ENTREGADORES` |

---

## Regras de Negócio para o ETL

- Somente pedidos com `status = 1` (entregue) são carregados na `fact_vendas`
- Pedidos com `status = 2` (cancelado) são excluídos da carga
- O `tempo_entrega_min` é calculado no ETL: `DATEDIFF(minute, dt_pedido, dt_entrega)`
- O estoque em `dim_produto` reflete o valor vigente no momento da carga (sem histórico — SCD Tipo 0)
- Clientes com `status = false` permanecem na dimensão para preservar o histórico de vendas

---

## Escopo do Projeto

Este projeto é um estudo de **modelagem dimensional** com foco em:

- Definição de negócio e levantamento de requisitos analíticos
- Modelagem Star Schema com uma única Fact Table
- Criação do OLTP normalizado como fonte de dados
- Desenvolvimento das queries de ETL (OLTP → DW)
- Construção de dashboard com as métricas definidas
