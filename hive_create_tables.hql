create database if not exists raw_gcat;
create database if not exists core_gcat;

CREATE TABLE if not exists raw_gcat.saint_marche(
  cod_loja string,
  loja string,
  bandeira string,
  uf string,
  cnpj string,
  ano string,
  mes string,
  sku string,
  produto string,
  marca string,
  fabricante string,
  segmento string,
  tamanho string,
  loja_critica string,
  venda_valor string,
  venda_unid string,
  venda_hectolitros string,
  tickets string)
partitioned by (localtimestamp varchar(50))  
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u003B'
  LINES TERMINATED BY '\n'
;


CREATE TABLE if not exists core_gcat.pr_sellout_saint_marche_historico(
geo    string
,nom_comercial    string
,codigo_cdd    string
,cdd    string
,uf    string
,sala    string
,mesa    string
,codigo_pdv    string
,cnpj_pdv    string
,pdv    string
,n_checkouts    string
,categoria    string
,tipoembalagem    string
,tamanho_embalagem    string
,fabricante    string
,marca    string
,familia    string
,versao    string
,segmento    string
,pack    string
,sku_id    string
,produto    string
,volume_sem2    double
,faturamento_sem2    double
,semana2_data    date
,fonte    string
,last_upd    date
,product_id    bigint
,subfonte    string
,match_unmatch    string,
localtimestamp string
)
;

CREATE TABLE if not exists core_gcat.pr_sellout_saint_marche_hist(
geo    string
,nom_comercial    string
,codigo_cdd    string
,cdd    string
,uf    string
,sala    string
,mesa    string
,codigo_pdv    string
,cnpj_pdv    string
,pdv    string
,n_checkouts    string
,categoria    string
,tipoembalagem    string
,tamanho_embalagem    string
,fabricante    string
,marca    string
,familia    string
,versao    string
,segmento    string
,pack    string
,sku_id    string
,produto    string
,volume_sem2    double
,faturamento_sem2    double
,semana2_data    date
,fonte    string
,last_upd    date
,product_id    bigint
,subfonte    string
,match_unmatch    string,
localtimestamp string
)
;



CREATE TABLE if not exists cubo_sellout_raw.pr_sellout_saint_marche(
geo    string
,nom_comercial    string
,codigo_cdd    string
,cdd    string
,uf    string
,sala    string
,mesa    string
,codigo_pdv    string
,cnpj_pdv    string
,pdv    string
,n_checkouts    string
,categoria    string
,tipoembalagem    string
,tamanho_embalagem    string
,fabricante    string
,marca    string
,familia    string
,versao    string
,segmento    string
,pack    string
,sku_id    string
,produto    string
,volume_sem2    double
,faturamento_sem2    double
,semana2_data    date
,fonte    string
,last_upd    date
,product_id    bigint
,subfonte    string
,match_unmatch    string
)
;


create table if not exists raw_gcat.processlog_saint_marche(
  sourcefile varchar(1000) ,
  targettable varchar(1000) ,
  semana_fim varchar(6),
  localtimestamp varchar(50))
;

create table if not exists core_gcat.processlog(
  sourcefile varchar(1000) ,
  targettable varchar(1000) ,
  semana_fim varchar(6),
  localtimestamp varchar(50))
;

