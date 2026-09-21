# 📊 MVP de Engenharia de Dados — Análise Olist E-commerce

> Pipeline de dados end-to-end (arquitetura medalhão) construído sobre o dataset público da **Olist**, um marketplace brasileiro, para responder perguntas de negócio sobre vendas, sellers, marketing e churn.

## 📑 Índice

- [Contexto](#-contexto)
- [Objetivos](#-objetivos)
- [Documentação](#-documentação)
- [Coleta de Dados](#-coleta-de-dados)
- [Modelagem de Dados](#modelagem-de-dados)
- [Carga e Pipeline dos Dados](#-carga-e-pipeline-dos-dados)
- [Qualidade dos Dados](#-qualidade-dos-dados)
- [Análise dos Dados](#-análise-dos-dados)
- [Auto-avaliação](#-auto-avaliação)
- [Databricks Jobs & Pipeline](#databricks-jobs-pipeline)

<br>

---

## 🧭 Contexto

Em um ambiente de negócios, análises comerciais e de marketing são essenciais para saber onde estão as oportunidades e riscos. Para este MVP, escolhi utilizar um dataset (**Olist E-commerce**, do Kaggle) que permitisse explorar um cenário parecido com o de um ambiente corporativo, transformando dados brutos em insights de performance.

A Olist é uma plataforma brasileira de marketplace que conecta pequenos e médios vendedores (*sellers*) a grandes canais de venda. O dataset contém o histórico de pedidos, além de dados de captação de vendedores para a plataforma.

### 🎯 Objetivos

Utilizando o dataset mencionado, o projeto busca responder às seguintes perguntas:

1. Quais são os meses de maior pico?
2. De onde vem a receita — a regra de Pareto se aplica aos vendedores da plataforma?
3. Quais segmentos de produto trazem mais receita?
4. Algum canal de marketing é melhor (em termos de conversão e ciclo médio de vendas)?
5. Existe relação entre baixo faturamento e maior risco de abandonar a plataforma? (LTV, churn)

> 💡 O dataset permitiria responder perguntas adicionais, mas para efeito deste MVP o escopo foi limitado às perguntas acima.

### 📚 Documentação

O pipeline de dados foi criado seguindo a **arquitetura medalhão**, que permite rastreabilidade dos dados desde seu estado bruto até a disponibilização para análise:

| Camada | Papel |
|---|---|
| 🥉 **Bronze** | Dados brutos, sem tratamento |
| 🥈 **Silver** | Dados padronizados e limpos |
| 🥇 **Gold** | Dados prontos para análise |

O catálogo de dados no Databricks segue essa mesma divisão (bronze / silver / gold). Os notebooks são organizados em `ingestion`, `bronze`, `silver`, `gold`, `analise` e `common` (funções compartilhadas entre os notebooks silver e gold).

<br>

---

## 📥 Coleta de Dados

Os arquivos utilizados neste projeto vêm do **Kaggle**, repositório que garante transparência quanto à licença de uso e origem dos dados, evitando problemas de privacidade e direitos autorais.

**Fontes:**
- [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)
- [Marketing Funnel by Olist](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist)

**📄 Licença:** ambos os datasets são disponibilizados sob **CC BY-NC-SA 4.0** (Atribuição, Uso Não-Comercial, Compartilhamento pela mesma licença), compatível com o uso acadêmico deste MVP.

**⚠️ Considerações:** Originalmente há mais planilhas disponíveis no Kaggle; para simplificação, foram incluídas apenas as que atendem aos objetivos deste trabalho.

Nem todas as colunas das tabelas abaixo são utilizadas no modelo final, já que não sã necessárias para responder as perguntas iniciais, que focam em vendas e indicadores de marketing. Elas foram descartadas na camada Silver e estão marcadas em *itálico* nas tabelas abaixo.

Clique nas tabelas abaixo para visualizar detalhes.

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

<br>

---
<a id="modelagem-de-dados"></a>

## 🏗️ Modelagem de Dados

### ⭐ Modelo Estrela (Star Schema)

Como a intenção é criar um ambiente OLAP, foi utilizada a modelagem estrela.
Inicialmente o modelo incluía dim_sellers e dim_customer, mas ambas foram removidas. O motivo é que somente uma coluna de cada tabelas era relevante às perguntas de negócio definidas no objetivo, tornando essas dimensões desnecessárias para este escopo. Assim, seller_id e customer_state foram mantidas como atributo direto na fato_vendas. Essa é uma decisão que pode ser revista caso análises futuras exijam mais granularidade (ex.: cidade do vendedor, número de compradores/clientes).

<img width="412" height="263" alt="image" src="https://github.com/user-attachments/assets/20a07c96-567c-44e2-ada9-5afe8da333ec" />

#### `fato_vendas`

Tabela fato de vendas com as métricas e dimensões necessárias para as análises comerciais.

| Coluna | Tipo | Descrição | Origem | Domínio |
|---|---|---|---|---|
| `order_id` | `STRING` | PK — ID único da ordem | `bronze.orders` | Identificador único alfanumérico |
| `mql_id` | `STRING` | FK do lead de marketing que originou essa venda, quando aplicável | `silver.closed_deals` via `LEFT JOIN` | Nullable; nem toda venda tem lead associado |
| `seller_id` | `STRING` | Vendedor na plataforma. Representa o real cliente da plataforma | `bronze.sellers` | Valor alfanumérico |
| `customer_state` | `STRING` | Estado onde o pedido foi realizado | `bronze.customers` | Sigla de estado brasileiro com 2 letras |
| `product_id` | `STRING` | FK do produto vendido | `bronze.order_items` | Valor alfanumérico |
| `order_date` | `DATE` | FK da data da compra | `bronze.orders` — `order_purchase_timestamp` | Formato `YYYY-MM-DD`; intervalo de `2016-09-04` a `2018-10-17` |
| `order_status` | `STRING` | Status atual da ordem no ciclo de vida do pedido | `bronze.orders` | `approved`, `canceled`, `created`, `delivered`, `invoiced`, `processing`, `shipped`, `unavailable` |
| `quantity` | `INT` | Quantidade de itens vendidos na ordem | `bronze.order_items` — `COUNT` agregado | Maior que `0`; número inteiro |
| `sales_value` | `DECIMAL` | Valor total vendido na ordem | `bronze.order_items` | Maior que `0`; intervalo de `0.85` a `6735.00` |
| `commission` | `DECIMAL` | Comissão da plataforma sobre a venda | Calculada como `sales_value * 0.1` | 10% de `sales_value` |

> 📌 **Premissa do MVP:** A taxa de 10% é uma simplificação adotada para este projeto — o dataset original não informa a comissão real da Olist. A coluna `commission` existe para viabilizar análises de receita da plataforma.

<br>

#### `dim_leads`

Todos os leads qualificados de marketing (MQLs), incluindo tanto os que converteram em sellers quanto os que não converteram. Consolida informações de leads e conversão em uma única dimensão para facilitar análises de funil completo.

**Justificativa da modelagem:** a Primary Key desta tabela é `mql_id`, pois é a única coluna sempre não-nula e única. `seller_id` não pode ser usado como PK porque é `NULL` para todos os leads não convertidos — assim como `won_date`, `sales_cycle`, `business_segment` e `lead_type`.

*Origem:* `bronze.marketing_qualified_leads` + `bronze.closed_deals` (para os convertidos).

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `mql_id` | `STRING` | PK — ID único do lead | Identificador único alfanumérico |
| `seller_id` | `STRING` | Vendedor para o qual o lead converteu, quando aplicável | Alfanumérico; `NULL` para leads não convertidos |
| `origin` | `STRING` | Canal pelo qual o lead chegou | `direct_traffic`, `display`, `email`, `organic_search`, `other`, `other_publicities`, `paid_search`, `referral`, `social`, `unknown` |
| `first_contact_date` | `DATE` | Data do primeiro contato | Formato `YYYY-MM-DD` |
| `won_date` | `DATE` | Data em que o lead se converteu em venda | Formato `YYYY-MM-DD`; `NULL` para leads não convertidos |
| `sales_cycle` | `INT` | Tempo, em dias, entre o primeiro contato e a conversão | Calculado como `DATEDIFF(won_date, first_contact_date)`; inteiro positivo; `NULL` para leads não convertidos |

<br>

#### `dim_products`

Produtos disponíveis na plataforma.

*Origem:* `silver.products`

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `product_id` | `STRING` | PK — ID único do produto | Identificador único alfanumérico |
| `category` | `STRING` | Categoria do produto | 74 categorias em português (ex.: `agro_industria_e_comercio`, `alimentos`, …); `"unknown"` para valores nulos |

<br>

#### `dim_dates`

Dimensão temporal para análises por período.

| Coluna | Tipo | Descrição | Origem | Domínio |
|---|---|---|---|---|
| `order_date` | `DATE` | PK — Data (YYYY-MM-DD) | Derivada de `order_purchase_timestamp` | Formato `YYYY-MM-DD`; intervalo de `2016-09-04` a `2018-10-17` |
| `year` | `INT` | Ano (YYYY) | Extraído de `order_date` | Intervalo: `2016–2018` |
| `month` | `INT` | Mês (1–12) | Extraído de `order_date` | `1–12` |
| `quarter` | `INT` | Trimestre (Q1–Q4) | Calculado a partir de `month` | `1–4` |
| `day_of_week` | `STRING` | Dia da semana | Extraído de `order_date` | `1–7` (`1 = Domingo`, `7 = Sábado`) |

<br>

---

## 🔄 Carga e Pipeline dos Dados

### 📥 Ingestão

Para evitar a necessidade de configurar credenciais de API do Kaggle diretamente no ambiente Databricks, os arquivos foram baixados manualmente e salvos no GitHub. O notebook de ingestão copia os CSVs do Git folder para o volume do Unity Catalog (`mvp_pucrio.raw_files.csv_files`).

Script com explicações: [`ingestion.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/ingestion.ipynb)

Visão final do volume raw_files.csv_files no Databricks:

<img width="530" height="513" alt="image" src="https://github.com/user-attachments/assets/185038e6-4f3b-43ce-bf29-39440443a462" />

<br>

### 🥉 Bronze

As tabelas foram criadas na camada bronze, em seu formato raw, mas com **tipos definidos** (schemas explícitos em PySpark para cada tabela). Cada tabela recebe duas colunas de controle: `_source_file` (nome do arquivo CSV de origem) e `_ingested_at` (timestamp da carga). As tabelas são nomeadas removendo prefixos (`olist_`) e sufixos (`_dataset`) dos arquivos originais.
Para escrever as tabelas, o modo overwrite foi utilizado neste MVP. No entanto, em um ambiente de produção, com atualizações recorrentes de dados, seria mais adequado usar uma estratégia incremental/MERGE, evitando reprocessar a base inteira a cada execução.

<br>

**Catálogo de Dados — Camada Bronze:**

Todas as tabelas e colunas foram documentadas no Unity Catalog via `COMMENT ON TABLE` e `COMMENT ON COLUMN`. Abaixo estão as descrições:

<br>
<details>
<summary><strong>📋 customers</strong> — Dados de clientes que realizaram compras na plataforma. Cada registro representa um cliente único identificado por customer_id.</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `customer_id` | `STRING` | ID único do cliente (PK) |
| `customer_unique_id` | `STRING` | ID único universal do cliente — usado para identificar o mesmo cliente em múltiplos pedidos |
| `customer_zip_code_prefix` | `INT` | Prefixo do CEP do cliente (primeiros 5 dígitos) |
| `customer_city` | `STRING` | Cidade de residência do cliente |
| `customer_state` | `STRING` | Estado (UF) de residência do cliente |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>
<br>
<details>
<summary><strong>📋 orders</strong> — Pedidos realizados pelos clientes. Contém informações de status e timestamps do ciclo de vida do pedido (compra, aprovação, envio, entrega).</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `order_id` | `STRING` | ID único do pedido (PK) |
| `customer_id` | `STRING` | ID do cliente que realizou o pedido (FK para customers) |
| `order_status` | `STRING` | Status atual do pedido (delivered, shipped, canceled, etc) |
| `order_purchase_timestamp` | `TIMESTAMP` | Data e hora da compra |
| `order_approved_at` | `TIMESTAMP` | Data e hora da aprovação do pagamento |
| `order_delivered_carrier_date` | `TIMESTAMP` | Data e hora da entrega ao transportador |
| `order_delivered_customer_date` | `TIMESTAMP` | Data e hora da entrega ao cliente |
| `order_estimated_delivery_date` | `TIMESTAMP` | Data estimada de entrega informada ao cliente |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>
<br>
<details>
<summary><strong>📋 order_items</strong> — Itens individuais de cada pedido. Um pedido pode conter múltiplos itens. Relaciona pedidos com produtos e sellers.</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `order_id` | `STRING` | ID do pedido (PK composta, FK para orders) |
| `order_item_id` | `INT` | Número sequencial do item dentro do pedido |
| `product_id` | `STRING` | ID do produto (PK composta, FK para products) |
| `seller_id` | `STRING` | ID do vendedor (PK composta, FK para sellers) |
| `shipping_limit_date` | `TIMESTAMP` | Data limite para o seller enviar o produto ao transportador |
| `price` | `DOUBLE` | Preço unitário do item (R$) |
| `freight_value` | `DOUBLE` | Valor do frete deste item (R$) |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>
<br>
<details>
<summary><strong>📋 products</strong> — Catálogo de produtos disponíveis na plataforma. Inclui características físicas e descritivas dos produtos.</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `product_id` | `STRING` | ID único do produto (PK) |
| `product_category_name` | `STRING` | Categoria do produto (em português) |
| `product_name_lenght` | `INT` | Comprimento do nome do produto (número de caracteres) |
| `product_description_lenght` | `INT` | Comprimento da descrição do produto (número de caracteres) |
| `product_photos_qty` | `INT` | Quantidade de fotos do produto |
| `product_weight_g` | `INT` | Peso do produto (gramas) |
| `product_length_cm` | `INT` | Comprimento do produto (cm) |
| `product_height_cm` | `INT` | Altura do produto (cm) |
| `product_width_cm` | `INT` | Largura do produto (cm) |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>
<br>
<details>
<summary><strong>📋 sellers</strong> — Vendedores/fornecedores cadastrados na plataforma. Cada seller pode vender múltiplos produtos.</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `seller_id` | `STRING` | ID único do seller/vendedor (PK) |
| `seller_zip_code_prefix` | `INT` | Prefixo do CEP do seller (primeiros 5 dígitos) |
| `seller_city` | `STRING` | Cidade onde o seller está localizado |
| `seller_state` | `STRING` | Estado (UF) onde o seller está localizado |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>
<br>
<details>
<summary><strong>📋 marketing_qualified_leads</strong> — Leads qualificados de marketing (MQLs). Prospects que demonstraram interesse e foram qualificados pelo time de marketing.</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `mql_id` | `STRING` | ID único do lead qualificado de marketing (PK) |
| `first_contact_date` | `DATE` | Data do primeiro contato com o lead |
| `landing_page_id` | `STRING` | ID da landing page de origem do lead |
| `origin` | `STRING` | Canal de origem do lead (organic_search, paid_search, social, etc) |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>
<br>
<details>
<summary><strong>📋 closed_deals</strong> — Negócios fechados. Contém informações sobre leads que se converteram em sellers ativos na plataforma.</summary>

| Coluna | Tipo | Descrição |
|---|---|---|
| `mql_id` | `STRING` | ID do lead (PK, FK para marketing_qualified_leads) |
| `seller_id` | `STRING` | ID do seller resultante da conversão (FK para sellers) |
| `sdr_id` | `STRING` | ID do SDR (Sales Development Representative) responsável |
| `sr_id` | `STRING` | ID do SR (Sales Representative) responsável |
| `won_date` | `TIMESTAMP` | Data e hora do fechamento do negócio |
| `business_segment` | `STRING` | Segmento de negócio do seller (pet, health_beauty, electronics, etc) |
| `lead_type` | `STRING` | Tipo/tamanho do lead (online_small, online_medium, online_big, etc) |
| `lead_behaviour_profile` | `STRING` | Perfil comportamental do lead durante o processo de vendas |
| `has_company` | `BOOLEAN` | Indica se o seller possui CNPJ |
| `has_gtin` | `BOOLEAN` | Indica se o seller possui código GTIN nos produtos |
| `average_stock` | `STRING` | Estoque médio declarado pelo seller |
| `business_type` | `STRING` | Tipo de negócio (reseller, manufacturer, etc) |
| `declared_product_catalog_size` | `DOUBLE` | Tamanho do catálogo de produtos declarado |
| `declared_monthly_revenue` | `DOUBLE` | Receita mensal declarada (R$) |
| `_source_file` | `STRING` | Nome do arquivo CSV de origem |
| `_ingested_at` | `TIMESTAMP` | Timestamp da carga |

</details>

O script para esta camada está em: [`bronze.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/bronze.ipynb)

Visão final da camada bronze no Databricks:

<img width="468" height="392" alt="image" src="https://github.com/user-attachments/assets/b081d00b-fe07-45f3-9a01-2d78118acf58" />

<br>

### 🥈 Silver

Antes de qualquer transformação, é feito um diagnóstico dos dados (nulos, duplicados, inconsistências) para saber o que precisa ser limpo.

<br>

**Diagnóstico (dados brutos na camada Bronze):**

| Tabela | Coluna | Nulos encontrados |
|---|---|---|
| `products` | `product_category_name` | 610 |
| `marketing_qualified_leads` | `origin` | 60 |
| Demais tabelas e colunas | — | 0 |

- **Duplicados:** nenhuma tabela apresentou duplicidade nas chaves primárias.
- **Consistência de valores categóricos:** também foram validados os estados (UFs brasileiras) em `customers` e `sellers`, e os status de pedido em `orders` (`delivered`, `shipped`, `canceled`, `processing`, `unavailable`, `invoiced`, `created`, `approved`). Nenhum valor inválido foi encontrado.
- **Agregação necessária em `order_items`:** 112.650 linhas na origem, mas apenas 102.425 combinações únicas de `order_id + product_id + seller_id`. As 10.225 linhas restantes representam múltiplos itens do mesmo produto/seller dentro do mesmo pedido, que foram agregados (somando `price` em `sales_value` e contando em `quantity`).

<br>

**Transformações aplicadas:**

1. Remoção de colunas fora do escopo do modelo final (ex.: `freight_value`, `shipping_limit_date`, `customer_unique_id`, etc.)
2. Agrupamento de `order_items` por `order_id + product_id + seller_id`, somando `price` em `sales_value` e contando linhas em `quantity` (112.650 → 102.425 linhas)
3. Substituição de valores ausentes por `"unknown"` (610 em `product_category_name`, 60 em `origin`)
4. Definição de tipos explícitos (ex.: `order_purchase_timestamp` → `DATE` como `order_date`)

<br>

**Verificação pós-limpeza:**

| Verificação | Resultado |
|---|---|
| Nulos em todas as tabelas silver | ✅ 0 |
| Duplicados em todas as tabelas silver | ✅ 0 |

<br>

**Checksum (validação do agrupamento):**

| Origem | Soma de `sales_value` |
|---|---|
| `bronze.order_items` (antes) | R$ 13.591.643,70 |
| `silver.order_items` (depois) | R$ 13.591.643,70 |
| **Diferença (checksum)** | **0** ✅ |

O agrupamento não alterou o valor total de vendas — nenhuma venda foi perdida ou duplicada.

<br>

Script: [`silver.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/silver.ipynb)

Visão final da camada silver no Databricks:

<img width="468" height="388" alt="image" src="https://github.com/user-attachments/assets/eaebac2b-927b-4ccc-9abe-72f5e9d8c6b2" />

<br>

### 🥇 Gold

Na camada gold, as tabelas são criadas e documentadas conforme o modelo estrela definido acima, com constraints de Primary Key/Foreign Key (apenas informativos — não verificados pelo Databricks).

<br>

**Criação de `dim_leads`** — o `LEFT JOIN` com `closed_deals` preserva os leads que não foram convertidos, preenchendo `seller_id` com `"unknown"` via `COALESCE`. Isso permite análises de funil completo — desde o primeiro contato até a conversão — sem perder leads que não avançaram no funil:

```sql
CREATE OR REPLACE TABLE dim_leads AS
SELECT 
    mql.mql_id,
    COALESCE(cd.seller_id, 'unknown') AS seller_id,
    mql.origin,
    mql.first_contact_date,
    CAST(cd.won_date AS DATE) AS won_date,
    DATEDIFF(CAST(cd.won_date AS DATE), mql.first_contact_date) AS sales_cycle
FROM silver.marketing_qualified_leads mql
LEFT JOIN silver.closed_deals cd
    ON mql.mql_id = cd.mql_id;
```

<br>

**Criação de `fato_vendas`** — a tabela fato envolve três joins:

1. `INNER JOIN` entre `orders` e `order_items` — cada linha da fato representa um item vendido dentro de uma ordem.
2. `LEFT JOIN` com `closed_deals` **no nível do seller** (por `seller_id`, não por `order_id`) — toda venda de um seller herda o mesmo `mql_id`, ou seja, o canal de aquisição é atribuído ao seller, não à venda individual. Sellers sem lead rastreado recebem `"unknown"`.
3. `LEFT JOIN` com `customers` (por `customer_id`) — traz `customer_state`, representando o local onde o produto foi vendido.

A comissão da plataforma é fixa em 10% sobre o valor da venda: `ROUND(sales_value * 0.10, 2)`.

```sql
CREATE OR REPLACE TABLE fato_vendas AS
SELECT 
    o.order_id,
    COALESCE(c.mql_id, "unknown") AS mql_id,
    oi.seller_id,
    cust.customer_state,
    oi.product_id,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date,
    COALESCE(o.order_status, 'unknown') AS order_status,
    oi.quantity,
    oi.sales_value,
    ROUND(oi.sales_value * 0.10, 2) AS commission
FROM silver.orders o
INNER JOIN silver.order_items oi 
    ON o.order_id = oi.order_id
LEFT JOIN silver.closed_deals c 
    ON oi.seller_id = c.seller_id
LEFT JOIN silver.customers cust
    ON o.customer_id = cust.customer_id;
```

<br>

**Checksum (validação dos JOINs):**

Comparando a soma de `sales_value` entre `silver.order_items` e `gold.fato_vendas`:

| Comparação | Resultado |
|---|---|
| `SUM(silver.order_items.sales_value) − SUM(fato_vendas.sales_value)` | **0** ✅ |

Os `JOIN`s não duplicaram nem perderam registros na tabela fato.

<br>

Script com explicações: [`gold.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/gold.ipynb) (inclui também a verificação de qualidade dos dados)

Visão final da camada gold no Databricks:

<img width="468" height="323" alt="image" src="https://github.com/user-attachments/assets/671d5c1a-c6e3-424c-bc95-c2613cbf5f6c" />

> 📌 As funções utilitárias compartilhadas entre silver e gold estão no notebook [`common.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/common.ipynb), que é executado dentro de cada notebook.

<br>

---

## ✅ Qualidade dos Dados

Foram verificadas completude, consistência, unicidade e acurácia dos dados, além da identificação de outliers que pudessem distorcer análises estatísticas. Nenhum ajuste foi realizado — tudo documentado no notebook `gold` (link acima).

<details>
<summary><strong>📋 Detalhes da verificação de qualidade</strong></summary>

| Dimensão | Resultado |
|---|---|
| **Completude** | ✅ Nenhuma tabela apresentou valores nulos nas colunas do modelo. A limpeza feita na camada Silver (valores ausentes preenchidos com `"unknown"`) garantiu completude até a camada final. |
| **Consistência** | ✅ Validada em dois níveis: (1) integridade referencial garantida pelas constraints de FK (`fk_fato_leads`, `fk_fato_produtos`, `fk_fato_dates`) e PK composta em `fato_vendas`; (2) checksum entre `silver.order_items` e `fato_vendas` confirmou que os joins não alteraram o valor total de vendas. A consistência de valores categóricos (estados, status de pedido) já foi verificada na camada Silver e chega à Gold por herança. |
| **Unicidade** | ✅ Não há linhas duplicadas nem violação da chave composta primária (`order_id + product_id + seller_id`) na tabela `fato_vendas`. |
| **Acurácia temporal** | ⚠️ `dim_leads` apresentou 17 registros de leads convertidos com `won_date` posterior à última data de venda registrada no dataset, e 1 registro com `won_date` anterior ao `first_contact_date` (inconsistência lógica — um negócio não pode ser fechado antes do primeiro contato). Optou-se por manter esses registros e documentar a limitação, já que representam menos de 0,1% da base de leads e não afetam as métricas de vendas (`fato_vendas`) — apenas análises específicas de ciclo de vendas que usem esses casos pontuais. |
| **Outliers** | ⚠️ 4.195 linhas (~4% de `fato_vendas`) têm preço unitário fora do intervalo IQR esperado para o respectivo produto. Ao inspecionar os casos de maior valor, eles correspondem a categorias coerentes com preços altos (eletrônicos, relógios, informática, ferramentas de construção), com `quantity=1` — sugerindo variação legítima de preço (versões/modelos diferentes do mesmo `product_id`, ou mudança de preço ao longo do tempo) e não erro de digitação. Optou-se por não remover essas linhas, mas registrar a decisão. |

<br>

**Código das verificações de acurácia temporal e outliers:**

```python
def check_temporal_accuracy(df, table_name, last_date):
    """Verifica se datas fazem sentido no contexto do negócio."""
    # 1. Datas no futuro (impossível)
    # 2. Won_date deve ser >= first_contact_date (para leads convertidos)
    ...

def detect_outliers(df, table_name):
    """Detecta outliers em preços unitários usando método IQR (Interquartile Range)."""
    # Calcula Q1, Q3 por produto e filtra linhas fora de [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
    ...
```

<br>

**Output da verificação:**

- Acurácia temporal: 17 linhas com `won_date` no futuro + 1 com `won_date < first_contact_date`
- Outliers: 4.195 linhas detectadas (categorias de alto preço: eletrônicos, relógios, informática)
- Resultado final: 🎉 Nenhuma tabela com problemas críticos — os encontrados foram documentados e mantidos

</details>

<br>

---

## 📈 Análise dos Dados

Script: [`analise.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/analise.ipynb)

Abaixo estão as respostas às perguntas iniciais do projeto, com insights e recomendações de ação.
<br>

### 1️⃣ Quais são os meses de maior pico?
<br>

<details>
<summary><strong>📊 Vendas mensais — receita total por mês</strong></summary>

<img width="573" height="244" alt="image" src="https://github.com/user-attachments/assets/e266d724-9e93-4d3d-a06c-78377fcf0667" />

</details>

**Novembro/2017 (R$ 1M)** — pico impulsionado pela Black Friday. As vendas crescem gradualmente de out/2016 até meados de 2018, mas caem 15% entre jul-set/2018 (de R$ 1M para R$ 850K), sugerindo sazonalidade ou perda de tração.

> 📊 **Período sugerido para análise:** set/2017 a ago/2018 (1 ano completo, após "estabilização" inicial da plataforma).

<br>

💡 **Recomendação:** Investigar se a queda em 2018 persiste nos meses seguintes para distinguir sazonalidade de perda de tração.

> ⚠️ Precisaríamos de um período maior (3 a 5 anos de 2018 em diante) para entender efetivamente se há picos de venda recorrentes. As **próximas análises** focam no período de setembro/2017 a agosto/2018, para ter um ano completo.

<br>

### 2️⃣ De onde vem a receita? A regra de Pareto se aplica aos vendedores?

**Sim.** 19% dos sellers (515 de 2.682) detêm 80% das vendas, confirmando Pareto. O seller #1 sozinho responde por R$ 203K (2% do total). Entre os top 10, o ticket médio varia de R$ 67 a R$ 581, revelando dois modelos de negócio distintos entre os maiores sellers. Como a comissão é fixa em 10%, os top sellers também são os que mais geram receita para a plataforma (top seller: R$ 20,4K de comissão no período). Os 81% restantes (2.167 sellers) dividem apenas 20% das vendas — longa cauda típica de marketplaces.


<br>
<details>
<summary><strong>📊 Sumário: Top 19% vs Restante 81%</strong></summary>

| Grupo | # Sellers | % Sellers | Pedidos | Vendas | % Vendas | Comissão | Ticket médio |
|---|---|---|---|---|---|---|---|
| Top 19% | 515 | 19,2% | 56.875 | R$ 8.341.945,67 | 80% | R$ 834.214,93 | R$ 330,01 |
| Restante 81% | 2.167 | 80,8% | 19.901 | R$ 2.086.911,79 | 20% | R$ 208.697,65 | R$ 158,90 |

</details>
<br>
<details>
<summary><strong>📊 Top sellers</strong></summary>


<img width="988" height="428" alt="image" src="https://github.com/user-attachments/assets/42c84960-3180-46c2-8d3c-a5cc53d755e2" />

</details>

<br>

💡 **Recomendação:** Foco em retenção e crescimento dos top sellers; estratégias para ativar a cauda longa de baixo volume.

<br>

### 3️⃣ Quais segmentos de produto trazem mais receita?

**beleza_saude lidera com R$ 1,01M** (7.056 pedidos).
<br> 

<details>
<summary><strong>📊 Categorias por receita</strong></summary>

<img width="746" height="389" alt="image" src="https://github.com/user-attachments/assets/0f1e7af7-1941-44f0-a1ba-a94caca37515" />

</details>

O top 5 representa ~43% do faturamento total — mix diversificado, sem dependência crítica de uma categoria. Utilidade doméstica, bem-estar e lazer dominam o catálogo mais vendido.

<br>

💡 **Recomendação:** Investir em categorias de alto ticket mas baixo volume (ex: PCs, R$ 1.223 de ticket médio) para aumentar receita sem prejudicar margem.

<br>

### 4️⃣ Algum canal de marketing é melhor (conversão e ciclo de vendas)?

**Paid_search (12,3%) e organic_search (11,8%)** lideram em conversão e volume de leads. Social tem conversão baixa (5,6%) mas traz 75 conversões pelo alto volume. Display tem ciclo curto (10 dias vs 50-60 dos líderes) mas apenas 6 conversões totais.

<details>
<summary><strong>📊 Output completo por canal</strong></summary>

<img width="987" height="255" alt="image" src="https://github.com/user-attachments/assets/b78591be-5cd3-41c1-8cd0-0595d158d5ef" />



> Canais `unknown`, `other` e `other_publicities` foram excluídos da análise por não serem rastreáveis.

</details>

<br>

💡 **Recomendações:**

- **Priorizar investimento em paid_search e organic_search** — melhor ROI (maior conversão + alto volume). Paid_search já traz 195 conversões; aumentar investimento pode escalar ainda mais.
- **Otimizar social** — tem volume alto (1.350 leads) mas conversão metade da média (5,6%). Melhorar qualidade dos leads ou jornada pós-clique pode dobrar as conversões sem aumentar custo de aquisição.
- **Reavaliar display** — ciclo curto (10 dias) é atrativo, mas conversão de 5,1% e apenas 6 conversões totais sugerem baixo retorno. Considerar realocar orçamento para canais de maior conversão.

<br>

### 5️⃣ Existe relação entre baixo faturamento e maior risco de churn? (LTV, churn)

**Sim, forte relação.** Sellers ativos (81,7%) têm LTV 6x maior que churned: R$ 459 vs R$ 74. Sellers ativos fazem 34 pedidos em média vs apenas 4 dos churned. O churn está associado a baixo engajamento nas primeiras vendas.

<details>
<summary><strong>📊 Output completo: Sellers ativos vs churned</strong></summary>

> Critério de churn: sem vendas há mais de 180 dias (referência: 31/08/2018).

<img width="886" height="116" alt="image" src="https://github.com/user-attachments/assets/92c66b25-ec5b-471e-b0ae-c69395ebe469" />


| Status | # Sellers | % Sellers | LTV médio (R$) | Pedidos/seller | Dias desde última venda |
|---|---|---|---|---|---|
| Active | 2.192 | 81,73% | 459,24 | 34,05 | 43,9 |
| Churned | 490 | 18,27% | 74,01 | 4,38 | 264,0 |

</details>

<br>

💡 **Recomendação:** Ações de suporte e onboarding focadas nas primeiras vendas para melhorar retenção de sellers novos.

<br>

---

## 📝 Auto-avaliação

Neste projeto, aprendi a utilizar o Databricks e a aplicar a arquitetura medalhão.
Consegui responder a maior parte das perguntas que faziam parte do objetivo deste trabalho, com a ressalva de que o período de tempo do dataset era curto, com o primeiro ano ainda mostrando um estágio de crescimento da plataforma.
Um dos problemas que enfrentei foi uma certa indecisão quanto a estrutura dos dados e quanto a apresentação dos notebooks.
Comecei o modelo star incluindo dim_customer e dim_sellers. No entanto, estas tabelas não eram realmente utilizadas - somente um campo de cada. Optei por remover, no entanto, talvez se no futuro buscasse mais granulosidade nas análises, seria interessante tê-las ali.
Quanto a apresentação, primeiro optei por um notebook que incluísse tudo. Mas ficou extremamente longo e poluído. No meio do projeto, decidi construir um notebook para cada etapa do projeto.
O que eu mais me marcou, no entanto, foi a experiência de utilizar IA como ferramenta de trabalho. Comecei pedindo à "Genie" que validasse como eu estava pensando em começar o projeto, mas recebi boa parte do código pronta. Minha primeira reação foi negativa, fiquei irritada porque o ponto do MVP era eu fazer o projeto. A Genie deletou tudo e passou a me acompanhar na construção. Com o tempo, percebi que tarefas repetitivas, particularmente a documentação de tabelas, podiam ser delegadas, sem que eu perdesse o controle. Me sentindo mais confortável com o Databricks e percebendo como o uso da IA economizava tempo, passei a usá-la com mais confiança, inclusive quando decidi reestruturar drasticamente os notebooks e remover tabelas. Não fosse a Genie, eu teria levado muito mais tempo para fazer estas mudanças.
Em uma indústria que valoriza experiência com IA na automação de projetos e análises, visto em quase todas as vagas de emprego na área, este projeto foi extremamente importante para mim. No final, senti que eu era a pessoa pensando e salvando tempo porque tinha IA para fazer o pesado.
Como trabalhos futuros, eu poderia:
- Reintroduzir dim_customer e dim_sellers caso análises futuras precisem de mais granularidade geográfica ou de atributos de seller;
- Migrar a carga da camada Bronze de overwrite para um modelo incremental/MERGE, mais adequado a um cenário de produção com atualizações recorrentes;
- Investigar um período de dados mais longo, para distinguir com mais confiança sazonalidade de queda real de tração da plataforma.

<br>

---
<a id="databricks-jobs-pipeline"></a>
## ⚙️ Usando Databricks Jobs & Pipeline

Por curiosidade, para automatizar a execução do pipeline de dados, foi criado um job utilizando a funcionalidade **Jobs & Pipelines** do Databricks.

<img width="548" height="254" alt="image" src="https://github.com/user-attachments/assets/666639ac-b7aa-4550-8b7b-ab2d8129c43f" />
