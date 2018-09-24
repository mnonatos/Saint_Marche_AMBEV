---- SCRIPT GCAT - saint_marche ----

---- carga da pr_sellout e distinct_product do cleansing

drop table if exists raw_gcat.tmp_saint_marche;
create temporary table if not exists raw_gcat.tmp_saint_marche (produto string,marca string,fabricante string,segmento string,tamanho string);
truncate table raw_gcat.tmp_saint_marche;

insert into raw_gcat.tmp_saint_marche
select distinct
  produto ,
  marca,
  fabricante,
  segmento,
  trim(ucase(nvl(regexp_replace(tamanho,',','\.'),''))) 
from raw_gcat.saint_marche a
join (select max(localtimestamp) as localtimestamp from raw_gcat.processlog_saint_marche) b
on a.localtimestamp = b.localtimestamp
;


----- retira possiveis duplicidades
drop table if exists raw_cleansing.distinct_products_tmp;
create temporary table raw_cleansing.distinct_products_tmp as
select categoria,fabr,marca,tamanho,prod,max(chave_produto) as chave_produto,last_upd_date,fonte
from raw_cleansing.distinct_products
where trim(ucase(fonte))='GCAT'
group by categoria,fabr,marca,tamanho,prod,last_upd_date,fonte
;


drop table if exists core_gcat.tmp2_saint_marche ;
create  table core_gcat.tmp2_saint_marche stored as sequencefile as select  a.*, b.chave_produto 
from raw_gcat.tmp_saint_marche a
left join raw_cleansing.distinct_products_tmp b 
on trim(ucase(a.produto))=trim(ucase(b.prod)) 
and trim(ucase(nvl(a.fabricante,'')))=trim(ucase(nvl(b.fabr,''))) 
and trim(ucase(nvl(a.marca,'')))=trim(ucase(nvl(b.marca,'')))  
and trim(ucase(nvl(a.tamanho,'')))=trim(ucase(nvl(b.tamanho,''))) 
and ucase(b.fonte)='GCAT';



drop table if exists core_gcat.pr_sellout_notfound_saint_marche ;
create  table core_gcat.pr_sellout_notfound_saint_marche stored as sequencefile  as 
select distinct
cast(null as string) as categoria,
fabricante as fabr,
marca,
tamanho,
produto as prod,
'GCAT' as fonte
from core_gcat.tmp2_saint_marche 
where chave_produto is null;


drop table if exists  core_gcat.pr_sellout_dist_prod_saint_marche;
create  table core_gcat.pr_sellout_dist_prod_saint_marche stored as sequencefile as 
select distinct
trim(ucase(categoria)) as categoria,
trim(ucase(fabr)) as fabr,
trim(ucase(marca)) as marca,
trim(tamanho) as tamanho,
trim(ucase(prod)) as prod,
row_number() over() + b.chave_produto as chave_produto,
c.localtimestamp,
'GCAT' as fonte
from  core_gcat.pr_sellout_notfound_saint_marche,
(select max(chave_produto) as chave_produto from raw_cleansing.distinct_products) b,
(select max(localtimestamp) as localtimestamp from raw_gcat.processlog_saint_marche) c;


------- Insere dados de produtos novos para o Cleansing
insert into raw_cleansing.distinct_products select * from core_gcat.pr_sellout_dist_prod_saint_marche;



drop table if exists core_gcat.pr_sellout_saint_marche_tmp1;
create table if not exists core_gcat.pr_sellout_saint_marche_tmp1 as 
SELECT
  dp.nom_geo as geo,
  dp.nom_comercial as nom_comercial,
  dp.cod_operacao as codigo_cdd,
  dp.nom_operacao as cdd,
  case when dp.estado is null  or trim(dp.estado) ='' then sm.uf else dp.estado end as uf,
  cast(null as string) as sala,
  cast(null as string) as mesa,
  dp.cod_pdv as codigo_pdv,
  trim(regexp_replace(regexp_replace(regexp_replace(sm.cnpj,'\\-',''),'\\/',''),'\\.','')) as cnpj_pdv,
  dp.nom_fantasia as pdv,
  cast(null as string) as n_checkouts,
  cast(null as string) as categoria,
  cast(null as string) as tipoembalagem,
  trim(ucase(regexp_replace(sm.tamanho,',','\.'))) as tamanho_embalagem,
  trim(sm.fabricante) as fabricante,
  ---case when sc.company is null or trim (sc.company)='' then sm.fabricante else sc.company end as fabricante,
  trim(sm.marca) as marca,
  ---case when sc.brand is null or trim (sc.brand)='' then sm.marca else sc.brand end as marca,
  cast(null as string) as familia,
  cast(null as string) as versao,
  case when trim (sm.segmento)='' then null else sm.segmento end as segmento,
  cast(null as string) as pack,
  case when trim (sm.sku)='' then null else sm.sku end as sku_id,
  case when trim (sm.produto)='' then null else sm.produto end as produto,
  cast(regexp_replace(sm.venda_hectolitros,',','.') as double) as volume_sem2,
  cast(regexp_replace(case when trim (sm.venda_valor)='' then null else sm.venda_valor end,'\\,','\\.') as double) as faturamento_sem2,
  cast(concat(trim(sm.ano),'-',trim(sm.mes),'-','01') as date) as semana2_data,
  cast('gcat' as string) as fonte,
  cast(concat(substring(pl.localtimestamp,1,4),'-',substring(pl.localtimestamp,5,2),'-',substring(pl.localtimestamp,7,2)) as date) as last_upd,
  case when sc.description is null then 'unmatched' else 'matched' end as match_unmatch,
  trim(sm.localtimestamp) as localtimestamp
FROM
raw_gcat.saint_marche as sm
inner join (select max(localtimestamp) as localtimestamp from raw_gcat.processlog_saint_marche) pl 
on sm.localtimestamp = pl.localtimestamp
left join raw_dimensions.dim_pdv as dp ON cast(trim(regexp_replace(regexp_replace(regexp_replace(sm.cnpj,'\\-',''),'\\/',''),'\\.','')) as bigint)= cast(trim(regexp_replace(regexp_replace(regexp_replace(dp.CNPJ,'\\-',''),'\\/',''),'\\.','')) as bigint)
left join raw_dimensions.from_to_brand_company sc on ucase(trim(sc.description))= ucase(trim(sm.produto))
;

---- retira possiveis duplicacoes
drop table if exists raw_cleansing.distinct_products_tmp2;
create temporary table raw_cleansing.distinct_products_tmp2 as
select categoria,fabr,marca,tamanho,prod,max(chave_produto) as chave_produto,last_upd_date,fonte
from raw_cleansing.distinct_products
where trim(ucase(fonte))='GCAT'
group by categoria,fabr,marca,tamanho,prod,last_upd_date,fonte
;


---- agrega product_id
drop table if exists core_gcat.pr_sellout_saint_marche_tmp2;
create table if not exists core_gcat.pr_sellout_saint_marche_tmp2 stored as sequencefile as 
SELECT  
    sm.geo,
    sm.nom_comercial,
    sm.codigo_cdd,
    sm.cdd,
    sm.uf,
    sm.sala,
    sm.mesa,
    sm.codigo_pdv,
    sm.cnpj_pdv,
    sm.pdv ,
    sm.n_checkouts,
    sm.categoria,
    sm.tipoembalagem,
    sm.tamanho_embalagem, 
    sm.fabricante,
    sm.marca,
    sm.familia,
    sm.versao,
    sm.segmento,
    sm.pack,
    sm.sku_id ,
    sm.produto ,
    sm.volume_sem2,
    sm.faturamento_sem2,
    sm.semana2_data ,
    sm.fonte, 
    sm.last_upd,
    d.chave_produto as product_id,
    'saint_marche' as subfonte,
    sm.match_unmatch,
	sm.localtimestamp
FROM core_gcat.pr_sellout_saint_marche_tmp1 as sm 
left join raw_cleansing.distinct_products_tmp2 d
on nvl(ucase(d.fonte),'')='GCAT'
and trim(ucase(nvl(sm.produto,'')))=trim(ucase(nvl(d.prod,'')))
and trim(ucase(nvl(sm.marca,'')))=trim(ucase(nvl(d.marca,'')))
and trim(regexp_replace(tamanho_embalagem,',','.'))=trim(regexp_replace(d.tamanho,',','.'))
and trim(ucase(nvl(sm.fabricante,'')))=trim(ucase(nvl(d.fabr,'')))
;


drop table if exists core_gcat.pr_sellout_hist_naobate_saint_marche;

create temporary table core_gcat.pr_sellout_hist_naobate_saint_marche as 
select a.* from core_gcat.pr_sellout_saint_marche_hist a 
left JOIN (select distinct cnpj_pdv, semana2_data, sku_id from core_gcat.pr_sellout_saint_marche_tmp2) b 
on a.semana2_data=b.semana2_data 
and nvl(trim(a.cnpj_pdv),'') = nvl(trim(b.cnpj_pdv),'')
and trim(a.sku_id) = trim(b.sku_id)
where b.semana2_data is null;


--- atualiza a tabela de historico do saint_marche no formato da pr_sellout
truncate table core_gcat.pr_sellout_saint_marche_hist;
insert into core_gcat.pr_sellout_saint_marche_hist select * from core_gcat.pr_sellout_hist_naobate_saint_marche;
insert into core_gcat.pr_sellout_saint_marche_hist select * from core_gcat.pr_sellout_saint_marche_tmp2 ;


--- insere dados do novo arquivo na pr_sellout sem tratamento do cleansing
insert into  cubo_sellout_raw.pr_sellout_saint_marche  
select 
geo
,nom_comercial
,codigo_cdd
,cdd
,uf
,sala
,mesa
,codigo_pdv
,cnpj_pdv
,pdv
,n_checkouts
,categoria
,tipoembalagem
,tamanho_embalagem
,fabricante
,marca
,familia
,versao
,segmento
,pack
,sku_id
,produto
,volume_sem2
,faturamento_sem2
,semana2_data
,fonte
,last_upd
,product_id
,subfonte
,match_unmatch
from core_gcat.pr_sellout_saint_marche_tmp2;


--- finalizacao de tabelas de controle

insert into table core_gcat.processlog
(sourcefile, targettable, semana_fim, localtimestamp)
select
sourcefile, 
targettable, 
semana_fim, 
localtimestamp
from raw_gcat.processlog_saint_marche;


truncate table raw_gcat.processlog_saint_marche;

