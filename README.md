# 📊 MVP de Engenharia de Dados — Análise Olist E-commerce

> Pipeline de dados end-to-end (arquitetura medalhão) construído sobre o dataset público da **Olist**, um marketplace brasileiro, para responder perguntas de negócio sobre vendas, sellers, marketing e churn.

## 📑 Índice

- [Contexto](#contexto)
- [Objetivos](#objetivos)
- [Documentação](#documentação)
- [Coleta de Dados](#coleta-de-dados)
- [Modelagem de Dados](#modelagem-de-dados)
- [Carga e Pipeline dos Dados](#carga-e-pipeline-dos-dados)
- [Qualidade dos Dados](#qualidade-dos-dados)
- [Análise dos Dados](#análise-dos-dados)
- [Auto-avaliação](#auto-avaliação)
- [Databricks Jobs & Pipeline](#usando-databricks-jobs--pipeline)

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

---

## 📥 Coleta de Dados

Os arquivos utilizados neste projeto vêm do **Kaggle**, repositório que garante transparência quanto à licença de uso e origem dos dados, evitando problemas de privacidade e direitos autorais.

**Fontes:**
- [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)
- [Marketing Funnel by Olist](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist)

**📄 Licença:** ambos os datasets são disponibilizados sob **CC BY-NC-SA 4.0** (Atribuição, Uso Não-Comercial, Compartilhamento pela mesma licença), compatível com o uso acadêmico deste MVP.

**⚠️ Considerações:** Originalmente há mais planilhas disponíveis no Kaggle; para simplificação, foram incluídas apenas as que atendem aos objetivos deste trabalho.

Nem todas as colunas das tabelas abaixo são utilizadas no modelo final — as descartadas na camada Silver estão marcadas em *itálico* nas tabelas abaixo.

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


---

## 🏗️ Modelagem de Dados

### ⭐ Modelo Estrela (Star Schema)

Como a intenção é criar um ambiente OLAP, foi utilizada a modelagem estrela:

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


#### `dim_products`

Produtos disponíveis na plataforma.

*Origem:* `silver.products`

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `product_id` | `STRING` | PK — ID único do produto | Identificador único alfanumérico |
| `category` | `STRING` | Categoria do produto | 74 categorias em português (ex.: `agro_industria_e_comercio`, `alimentos`, …); `"unknown"` para valores nulos |


#### `dim_dates`

Dimensão temporal para análises por período.

| Coluna | Tipo | Descrição | Origem | Domínio |
|---|---|---|---|---|
| `order_date` | `DATE` | PK — Data (YYYY-MM-DD) | Derivada de `order_purchase_timestamp` | Formato `YYYY-MM-DD`; intervalo de `2016-09-04` a `2018-10-17` |
| `year` | `INT` | Ano (YYYY) | Extraído de `order_date` | Intervalo: `2016–2018` |
| `month` | `INT` | Mês (1–12) | Extraído de `order_date` | `1–12` |
| `quarter` | `INT` | Trimestre (Q1–Q4) | Calculado a partir de `month` | `1–4` |
| `day_of_week` | `STRING` | Dia da semana | Extraído de `order_date` | `1–7` (`1 = Domingo`, `7 = Sábado`) |


---

## 🔄 Carga e Pipeline dos Dados

### 📥 Ingestão

Para evitar a necessidade de configurar credenciais de API do Kaggle diretamente no ambiente Databricks, os arquivos foram baixados manualmente e salvos no GitHub. O notebook de ingestão copia os CSVs do Git folder para o volume do Unity Catalog (`mvp_pucrio.raw_files.csv_files`).

Script com explicações: [`ingestion.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/ingestion.ipynb)

Visão final do volume raw_files.csv_files no Databricks:

<img width="530" height="513" alt="image" src="https://github.com/user-attachments/assets/185038e6-4f3b-43ce-bf29-39440443a462" />


### 🥉 Bronze

Nesta camada, as tabelas são criadas na camada bronze, em seu formato raw, mas com tipos definidos.
O script para esta camada está em: [`bronze.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/bronze.ipynb)

Visão final da camada bronze no Databricks:

<img width="468" height="392" alt="image" src="https://github.com/user-attachments/assets/b081d00b-fe07-45f3-9a01-2d78118acf58" />

### 🥈 Silver

Antes de qualquer transformação, é feito um diagnóstico dos dados (nulos, duplicados, inconsistências) para saber o que precisa ser limpo. Em seguida:

1. Remoção de colunas fora do escopo do modelo final
2. Remoção de duplicados
3. Substituição de valores ausentes por `"unknown"`
4. Nova verificação de qualidade após a limpeza

Script com explicações: [`silver.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/silver.ipynb)

Visão final da camada silver no Databricks:

<img width="468" height="388" alt="image" src="https://github.com/user-attachments/assets/eaebac2b-927b-4ccc-9abe-72f5e9d8c6b2" />


### 🥇 Gold

Na camada gold, as tabelas são criadas e documentadas conforme o modelo estrela definido acima, com constraints de Primary Key/Foreign Key (apenas informativos — não verificados pelo Databricks).

Também é implementado um **checksum de validação**, comparando a soma de `sales_value` em `fato_vendas` com a soma original em `order_items`, garantindo que os `JOIN`s não duplicaram registros na tabela fato.

Script com explicações: [`gold.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/gold.ipynb) (inclui também a verificação de qualidade dos dados)

Visão final da camada gold no Databricks:

<img width="468" height="323" alt="image" src="https://github.com/user-attachments/assets/671d5c1a-c6e3-424c-bc95-c2613cbf5f6c" />

> 📌 As funções utilitárias compartilhadas entre silver e gold estão no notebook [`common.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/common.ipynb), que é executado dentro de cada notebook.

---

## ✅ Qualidade dos Dados

Foram verificadas completude, consistência, unicidade e acurácia dos dados, além da identificação de outliers que pudessem distorcer análises estatísticas. Nenhum ajuste foi realizado — tudo documentado no notebook `gold` (link acima).

---

## 📈 Análise dos Dados

Script com explicações: [`analise.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/analise.ipynb)

> 💡 **Nota:** Abaixo está um resumo das respostas. A análise completa com insights detalhados e recomendações de ação está disponível no notebook [`analise.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/analise.ipynb).

Abaixo estão as respostas às perguntas iniciais do projeto.

### 1️⃣ Quais são os meses de maior pico?

**Novembro/2017 (R$ 1M)** — pico impulsionado pela Black Friday. As vendas crescem gradualmente de out/2016 até meados de 2018, mas caem 15% entre jul-set/2018 (de R$ 1M para R$ 850K), sugerindo sazonalidade ou perda de tração.

> 📊 **Período sugerido para análise:** set/2017 a ago/2018 (1 ano completo, após "estabilização" inicial da plataforma).

💡 **Recomendação:** Investigar se a queda em 2018 persiste nos meses seguintes para distinguir sazonalidade de perda de tração.

### 2️⃣ De onde vem a receita? A regra de Pareto se aplica aos vendedores?

**Sim.** 19% dos sellers (516 de 2.682) detêm 80% das vendas, confirmando Pareto. O seller #1 sozinho responde por R$ 203K (2% do total). Os 81% restantes dividem apenas 20% das vendas — longa cauda típica de marketplaces.

💡 **Recomendação:** Foco em retenção e crescimento dos top sellers; estratégias para ativar a cauda longa de baixo volume.

### 3️⃣ Quais segmentos de produto trazem mais receita?

**beleza_saude lidera com R$ 1,01M** (7.056 pedidos). Top 5:

| Categoria | Receita | Ticket médio |
|---|---|---|
| 🥇 beleza_saude | R$ 1,01M | R$ 139,50 |
| 🥈 relogios_presentes | R$ 991K | R$ 199,88 |
| 🥉 cama_mesa_banho | R$ 777K | R$ 100,45 |
| 4️⃣ esporte_lazer | R$ 757K | R$ 126,40 |
| 5️⃣ informatica_acessorios | R$ 689K | R$ 127,89 |

O top 5 representa ~43% do faturamento total — mix diversificado, sem dependência crítica de uma categoria.

💡 **Recomendação:** Investir em categorias de alto ticket mas baixo volume (ex: PCs, R$ 1.223 de ticket médio) para aumentar receita sem prejudicar margem.

### 4️⃣ Algum canal de marketing é melhor (conversão e ciclo de vendas)?

**Paid_search (12,3%) e organic_search (11,8%)** lideram em conversão e volume de leads. Social tem conversão baixa (5,6%) mas traz 75 conversões pelo alto volume. Display tem ciclo curto (10 dias vs 50-60 dos líderes) mas apenas 6 conversões totais.

💡 **Recomendação:** Priorizar investimento em paid/organic search; otimizar social (alto volume, baixa conversão); reavaliar display (baixo retorno).

### 5️⃣ Existe relação entre baixo faturamento e maior risco de churn? (LTV, churn)

**Sim, forte relação.** Sellers ativos (81,7%) têm LTV 6x maior que churned: R$ 459 vs R$ 74. Sellers ativos fazem 34 pedidos em média vs apenas 4 dos churned. O churn está associado a baixo engajamento nas primeiras vendas.

💡 **Recomendação:** Ações de suporte e onboarding focadas nas primeiras vendas para melhorar retenção de sellers novos.

---

## 📝 Auto-avaliação

Neste projeto, aprendi a utilizar o Databricks e a aplicar a arquitetura medalhão. Percebi que a parte mais importante de um projeto de Engenharia de Dados é começar com o porquê para depois se importar com o como.
Consegui responder a maior parte das perguntas que faziam parte do objetivo deste trabalho, com a ressalva de que o período de tempo do dataset era curto, com o primeiro ano ainda mostrando um estágio de crescimento da plataforma.
O que eu mais me marcou, no entanto, foi a experiência de utilizar IA como ferramenta de trabalho. Comecei pedindo à "Genie" que validasse como eu estava pensando em começar o projeto, mas recebi boa parte do código pronta. Minha primeira reação foi negativa, fiquei irritada porque o ponto do MVP era eu fazer o projeto. A Genie deletou tudo e passou a me acompanhar na construção. Com o tempo, percebi que tarefas repetitivas, particularmente a documentação de tabelas, podiam ser delegadas, sem que eu perdesse o controle. Me sentindo mais confortável com o Databricks e percebendo como o uso da IA economizava tempo, passei a usá-la com mais confiança, inclusive quando decidi reestruturar drasticamente os notebooks (de um único para múltiplos).
Em uma indústria que valoriza experiência com IA na automação de projetos e análises, visto em quase todas as vagas de emprego na área, este projeto foi extremamente importante para mim. No final, senti que eu era a pessoa pensando e salvando tempo porque tinha IA para fazer o pesado.

---

## ⚙️ Usando Databricks Jobs & Pipeline

Por curiosidade, para automatizar a execução do pipeline de dados, foi criado um job utilizando a funcionalidade **Jobs & Pipelines** do Databricks.

<img width="548" height="254" alt="image" src="https://github.com/user-attachments/assets/666639ac-b7aa-4550-8b7b-ab2d8129c43f" />
