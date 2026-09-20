# 📊 MVP de Engenharia de Dados — Análise Olist E-commerce

> Pipeline de dados end-to-end (arquitetura medalhão) construído sobre o dataset público da **Olist**, um marketplace brasileiro, para responder perguntas de negócio sobre vendas, sellers, marketing e churn.

## 📑 Índice

- [Contexto](#-contexto)
- [Objetivos](#-objetivos)
- [Documentação](#-documentação)
- [Coleta de Dados](#-coleta-de-dados)
- [Modelagem de Dados](#-modelagem-de-dados)
- [Carga e Pipeline dos Dados](#-carga-e-pipeline-dos-dados)
- [Qualidade dos Dados](#-qualidade-dos-dados)
- [Análise dos Dados](#-análise-dos-dados)
- [Databricks Jobs & Pipeline](#-usando-databricks-jobs--pipeline)

---

## 🧭 Contexto

Em um ambiente de negócios, análises comerciais e de marketing são essenciais para saber onde estão as oportunidades e riscos. Para este MVP, escolhi utilizar um dataset (**Olist E-commerce**, do Kaggle) que permitisse explorar um cenário parecido com o de um ambiente corporativo, transformando dados brutos em insights de performance.

A Olist é uma plataforma brasileira de marketplace que conecta pequenos e médios vendedores (*sellers*) a grandes canais de venda. O dataset contém o histórico de pedidos, além de dados de captação de vendedores para a plataforma.

## 🎯 Objetivos

Utilizando o dataset mencionado, o projeto busca responder às seguintes perguntas:

1. Quais são os meses de maior pico?
2. De onde vem a receita — a regra de Pareto se aplica aos vendedores da plataforma?
3. Quais segmentos de produto trazem mais receita?
4. Algum canal de marketing é melhor (em termos de conversão e ciclo médio de vendas)?
5. Existe relação entre baixo faturamento e maior risco de abandonar a plataforma? (LTV, churn)

> 💡 O dataset permitiria responder perguntas adicionais, mas para efeito deste MVP o escopo foi limitado às perguntas acima.

## 📚 Documentação

O pipeline de dados foi criado seguindo a **arquitetura medalhão**, que permite rastreabilidade dos dados desde seu estado bruto até a disponibilização para análise:

| Camada | Papel |
|---|---|
| 🥉 **Bronze** | Dados brutos, sem tratamento |
| 🥈 **Silver** | Dados padronizados e limpos |
| 🥇 **Gold** | Dados prontos para análise |

O catálogo de dados no Databricks segue essa mesma divisão (bronze / silver / gold). Os notebooks são organizados em `bronze`, `silver`, `gold`, `analise` e `common` (funções compartilhadas entre os notebooks silver e gold).

---

## 📥 Coleta de Dados

Os arquivos utilizados neste projeto vêm do **Kaggle**, repositório que garante transparência quanto à licença de uso e origem dos dados, evitando problemas de privacidade e direitos autorais.

**Fontes:**
- [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)
- [Marketing Funnel by Olist](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist)

**📄 Licença:** ambos os datasets são disponibilizados sob **CC BY-NC-SA 4.0** (Atribuição, Uso Não-Comercial, Compartilhamento pela mesma licença), compatível com o uso acadêmico deste MVP.

Os arquivos foram salvos em `mvp_pucrio.raw_files.csv_files` (e também na pasta [`files`](./files) deste repositório). Nem todas as colunas são utilizadas no modelo final — as descartadas na camada Silver estão marcadas em *itálico* nas tabelas abaixo.

<br>

<details>
<summary><strong>📁 olist_order_items_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| order_id | string |
| *order_item_id* | int |
| product_id | string |
| seller_id | string |
| *shipping_limit_date* | timestamp |
| price | double |
| *freight_value* | double |

</details>
<br>
<details>
<summary><strong>📁 olist_customers_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| customer_id | string |
| *customer_unique_id* | string |
| customer_zip_code_prefix | int |
| customer_city | string |
| customer_state | string |

</details>
<br>
<details>
<summary><strong>📁 olist_orders_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| order_id | string |
| customer_id | string |
| order_status | string |
| order_purchase_timestamp | timestamp |
| *order_approved_at* | timestamp |
| *order_delivered_carrier_date* | timestamp |
| *order_delivered_customer_date* | timestamp |
| *order_estimated_delivery_date* | timestamp |

</details>
<br>
<details>
<summary><strong>📁 olist_sellers_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| seller_id | string |
| *seller_zip_code_prefix* | int |
| seller_city | string |
| seller_state | string |

</details>
<br>
<details>
<summary><strong>📁 olist_marketing_qualified_leads_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| mql_id | string |
| first_contact_date | date |
| *landing_page_id* | string |
| origin | string |

</details>
<br>
<details>
<summary><strong>📁 olist_closed_deals_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| mql_id | string |
| seller_id | string |
| sdr_id | string |
| sr_id | string |
| won_date | timestamp |
| business_segment | string |
| lead_type | string |
| *lead_behaviour_profile* | string |
| *has_company* | boolean |
| *has_gtin* | boolean |
| *average_stock* | string |
| *business_type* | string |
| *declared_product_catalog_size* | double |
| *declared_monthly_revenue* | double |

</details>
<br>
<details>
<summary><strong>📁 olist_products_dataset.csv</strong></summary>

| Coluna | Tipo |
|---|---|
| product_id | string |
| product_category_name | string |
| *product_name_lenght* | int |
| *product_description_lenght* | int |
| *product_photos_qty* | int |
| *product_weight_g* | int |
| *product_length_cm* | int |
| *product_height_cm* | int |
| *product_width_cm* | int |

</details>

**⚠️ Considerações:**

1. Assume-se que a receita da plataforma vem de comissões por venda efetuada (10% do valor total do pagamento).
2. Os "vendedores"/sellers são na verdade os clientes da plataforma.
3. Originalmente há mais planilhas disponíveis no Kaggle; para simplificação, foram incluídas apenas as que atendem aos objetivos deste trabalho.

> 🚧 **Pontos em aberto no rascunho original** (mantidos aqui para revisão, não removi porque pareciam anotações suas para revisar depois):
> - Detalhar melhor o que significa "vendedores são os clientes da plataforma" (item 2 acima estava incompleto no original).
> - Decidir se a explicação sobre comissões deveria vir antes, na seção de Contexto/Objetivos.

---

## 🏗️ Modelagem de Dados

### ⭐ Modelo Estrela (Star Schema)

Como a intenção é criar um ambiente OLAP, foi utilizada a modelagem estrela:

![](path)

#### `fato_vendas`

Tabela de transações de vendas com todas as métricas agregáveis.

![fato_vendas](./media/image2.png)

#### `dim_leads`

Todos os leads qualificados de marketing (MQLs), incluindo tanto os que converteram em sellers quanto os que não converteram. Consolida informações de leads e conversão em uma única dimensão para facilitar análises de funil completo.

**Justificativa da modelagem:** a Primary Key desta tabela é `mql_id`, pois é a única coluna sempre não-nula e única. `seller_id` não pode ser usado como PK porque é `NULL` para todos os leads não convertidos — assim como `won_date`, `sales_cycle`, `business_segment` e `lead_type`.

*Origem:* `bronze.marketing_qualified_leads` + `bronze.closed_deals` (para os convertidos).

![dim_leads](./media/image3.png)

#### `dim_products`

Produtos disponíveis na plataforma.

*Origem:* `silver.products`

![dim_products](./media/image4.png)

#### `dim_dates`

Dimensão temporal para análises por período.

![dim_dates](./media/image5.png)

---

## 🔄 Carga e Pipeline dos Dados

### Carga dos dados

Para evitar a necessidade de configurar credenciais de API do Kaggle diretamente no ambiente Databricks, os arquivos foram baixados manualmente e salvos no GitHub; de lá, foram carregados como volume no catálogo do Databricks (`mvp_pucrio.raw_files.csv_files`).

### 🥉 Bronze

Scripts: [`bronze.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/bronze.ipynb)

Visão final da camada bronze no Databricks:

![Camada Bronze](./media/image6.png)

### 🥈 Silver

Antes de qualquer transformação, é feito um diagnóstico dos dados (nulos, duplicados, inconsistências) para saber o que precisa ser limpo. Em seguida:

1. Remoção de colunas fora do escopo do modelo final
2. Remoção de duplicados
3. Substituição de valores ausentes por `"unknown"`
4. Nova verificação de qualidade após a limpeza

Scripts: [`silver.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/silver.ipynb)

Visão final da camada silver no Databricks:

![Camada Silver](./media/image7.png)

### 🥇 Gold

Na camada gold, as tabelas são criadas e documentadas conforme o modelo estrela definido acima, com constraints de Primary Key/Foreign Key (apenas informativos — não verificados pelo Databricks).

Também é implementado um **checksum de validação**, comparando a soma de `sales_value` em `fato_vendas` com a soma original em `order_items`, garantindo que os `JOIN`s não duplicaram registros na tabela fato.

Scripts: [`gold.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/gold.ipynb) (inclui também a verificação de qualidade dos dados)

---

## ✅ Qualidade dos Dados

Foram verificadas completude, consistência, unicidade e acurácia dos dados, além da identificação de outliers que pudessem distorcer análises estatísticas. Nenhum ajuste foi necessário — tudo documentado no notebook `gold` (link acima).

---

## 📈 Análise dos Dados

Scripts: [`analise.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/analise.ipynb)

Abaixo estão as respostas às perguntas iniciais do projeto.

### 1️⃣ Quais são os meses de maior pico?

O total vendido vem aumentando gradativamente desde outubro/2016 até meados de 2018, com pico de vendas em **novembro/2017 (R$ 1M)** — provavelmente puxado pela Black Friday.

> ⚠️ **Ponto de atenção:** entre julho e setembro/2018, as vendas caem de ~R$ 1M para ~R$ 850K (-15%). Pode indicar sazonalidade natural ou perda de tração da plataforma — vale investigar se a queda persiste nos meses seguintes.

Seria necessário um período maior (3 a 5 anos a partir de 2018) para confirmar picos de venda recorrentes. Para as **próximas análises**, o foco será o período de setembro/2017 a agosto/2018 (um ano completo), já que o período anterior parece ser o de início dos vendedores na plataforma, antes da estabilização das vendas.

### 2️⃣ De onde vem a receita? A regra de Pareto se aplica aos vendedores?

**516 dos 2.682 vendedores (19%)** detêm aproximadamente **80% das vendas**, confirmando a regra 80/20 de Pareto. Apenas **123 sellers (5%)** acumulam 50% do faturamento, enquanto o seller #1 sozinho responde por ~2% de todo o volume (R$ 203K em um ano).

Entre os top 10, o ticket médio varia de **R$ 67** (alto volume, baixo valor por ordem) a **R$ 581** (baixo volume, alto valor por ordem) — dois modelos de negócio distintos entre os maiores sellers. Como a comissão é fixa em 10%, os top sellers também são os que mais geram receita para a plataforma (top seller: R$ 20,4K de comissão no período).

Os 81% restantes (2.166 sellers) dividem apenas 20% das vendas — uma longa cauda de vendedores de baixo volume, padrão típico de marketplaces digitais.

### 3️⃣ Quais segmentos de produto trazem mais receita?

| Categoria | Receita | Pedidos | Ticket médio |
|---|---|---|---|
| 🥇 beleza_saude | R$ 1M | 7.043 | R$ 139,61 |
| 🥈 relogios_presentes | R$ 988K | 4.802 | R$ 199,77 |
| 🥉 cama_mesa_banho, esporte_lazer, informática | — | — | — |

`relogios_presentes` tem ticket médio bem maior que `beleza_saude`, indicando uma categoria mais cara e de menor volume. O top 5 confirma que utilidade doméstica, bem-estar e lazer dominam o catálogo mais vendido.

### 4️⃣ Algum canal de marketing é melhor (conversão e ciclo de vendas)?

| Canal | Conversão | Observação |
|---|---|---|
| Pesquisas pagas | 12,3% | Maior conversão e nº de leads |
| Buscas orgânicas | 11,8% | Também líder em leads |
| Tráfego direto | 11,2% | — |
| Social | 5,6% | Baixa conversão, mas alto volume (75 leads convertidos) |
| Display | 5,1% | Ciclo de vendas mais curto (10,3 dias) |
| Email | 3,0% | Menor conversão |

Apesar da conversão baixa, **social** traz mais leads convertidos (75) do que **referral** (23), que tem taxa de conversão um pouco maior. **Display** tem o ciclo de vendas mais curto (10,3 dias, contra 50–60 dias de paid/organic search), mas sua baixa conversão sugere que a rapidez não compensa o volume perdido.

### 5️⃣ Existe relação entre baixo faturamento e maior risco de churn? (LTV, churn)

Dos 2.682 sellers, **2.192 (81,7%) estão ativos** e **490 (18,3%) estão churned** (sem vendas há mais de 180 dias, referência 31/08/2018).

| Status | LTV médio (comissão) | Pedidos médios |
|---|---|---|
| ✅ Ativo | R$ 459 | 34 |
| ❌ Churned | R$ 74 | 4 |

O LTV médio de um seller ativo é **~6x maior** que o de um churned, e sellers ativos vendem muito mais. Isso indica que o churn está associado a baixo engajamento — sellers que não conseguem crescer as vendas tendem a abandonar a plataforma, sugerindo que ações de suporte nas primeiras vendas poderiam impactar a retenção.

---

## ⚙️ Usando Databricks Jobs & Pipeline

Por curiosidade, para automatizar a execução do pipeline de dados, foi criado um job utilizando a funcionalidade **Jobs & Pipelines** do Databricks.

**Ordem dos notebooks:**

![Ordem dos notebooks](./media/image8.png)

**Execuções (rodadas):**

![Rodadas do pipeline](./media/image9.png)
