-- quem, quando, o quê, onde?

create table dim_tempo(
  sk_tempo INT GENERATED always as identity primary key,
  dt_referencia date not null,
  hora smallint not null,
  ano smallint not null,
  trimestre smallint not null,
  mes smallint not null,
  nm_mes varchar(20) not null,
  semana_ano SMALLINT NOT NULL,
  dia_mes smallint not null,
  nm_dia_semana varchar(20) not null,
  fim_de_semana boolean not null 
);

create table dim_produto(
  sk_produto INT generated always as identity primary key,
  id_produto int not null,
  nm_produto varchar(250) not null,
  nm_categoria varchar(100) not null,
  preco_tabela decimal(10, 2) not null,
  custo_unitario decimal(10, 2) not null,
  estoque_atual int not null,
  estoque_minimo int not null,
  ativo boolean not null
);

create table dim_cliente (
  sk_cliente int generated always as identity primary key,
  id_cliente int not null,
  nm_cliente varchar(200) not null,
  cidade varchar(200),
  uf varchar(2),
  dt_cadastro date not null,
  status boolean not null,
  data_churn date
);

create table dim_loja (
  sk_loja int generated always as identity primary key,
  id_loja int not null,
  nm_loja varchar(200) not null,
  cidade varchar(200) not null,
  uf varchar(2) not null
);

create table dim_entregador (
  sk_entregador int generated always as identity primary key,
  id_entregador int not null,
  nm_entregador varchar(200) not null,
  nm_loja_base varchar(200) not null,
  dt_admissao date not null,
  ativo boolean not null
);

create table fact_venda (
  id int generated always as identity primary key,
  sk_tempo int not null,
  sk_produto int not null,
  sk_cliente int not null,
  sk_loja int not null,
  sk_entregador int not null,

  qtd_itens int not null,
  valor_receita_bruta decimal(10, 2) not null,
  valor_desconto decimal(10, 2) not null,
  valor_receita_liq decimal(10, 2) not null,
  valor_custo decimal(10, 2) not null,
  tempo_entrega_min int,

  -- foreign keys
  CONSTRAINT fk_fact_tempo
    FOREIGN KEY (sk_tempo)      REFERENCES dim_tempo(sk_tempo),
  CONSTRAINT fk_fact_produto
    FOREIGN KEY (sk_produto)    REFERENCES dim_produto(sk_produto),
  CONSTRAINT fk_fact_cliente
    FOREIGN KEY (sk_cliente)    REFERENCES dim_cliente(sk_cliente),
  CONSTRAINT fk_fact_loja
    FOREIGN KEY (sk_loja)       REFERENCES dim_loja(sk_loja),
  CONSTRAINT fk_fact_entregador
    FOREIGN KEY (sk_entregador) REFERENCES dim_entregador(sk_entregador)
);

ALTER TABLE fact_venda
ADD CONSTRAINT uq_fact_venda
UNIQUE (sk_tempo, sk_produto, sk_cliente, sk_loja, sk_entregador);
