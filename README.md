# README - Projeto de Ingestão de Dados (PostgreSQL, Pandas & Psycopg2)

## 💡 Sobre o Projeto

Este projeto é uma solução de **ETL (Extract, Transform, Load)** desenvolvida em Python para automatizar a ingestão e normalização de dados de diferentes fontes (`.xlsx` e `.csv`) diretamente para um banco de dados **PostgreSQL**.

O objetivo principal é garantir a **integridade dos dados** através de regras de limpeza robustas, convertendo formatos inconsistentes (datas, CNPJs, percentuais) em tipos de dados adequados para o armazenamento e análise no banco.

---

## 🎯 Problema e Solução

### 1. 🔄 Ingestão e Normalização de Dados de Seguro (Excel)

| Script | `integracao_py_dbeaver_xlsx.py` |
| :--- | :--- |
| **Problema Curado** | Importar a base de apólices de seguro (`clt_seguro.xlsx`) com **inconsistências críticas** de formato: CNPJs com pontuação, percentuais como *string* (`5,00%`), e datas de vigência em múltiplos formatos ou inválidas. |
| **Solução Resolvida** | O script aplica um **tratamento robusto por campo**: <ul><li>**CNPJ:** Limpeza total de caracteres especiais e conversão para `int`.</li><li>**Percentuais:** Substituição de vírgula por ponto, remoção de '%' e conversão para `float` (dividido por 100).</li><li>**Data de Vigência:** Tenta converter a *string* para data nos formatos `DD/MM/YYYY` e `YYYY-MM-DD`. Se falhar, insere `NULL`, evitando erros na coluna `DATE` do banco.</li><li>**Performance:** Utiliza `psycopg2.extras.execute_batch` para inserção eficiente em lote.</li></ul> |

### 2. 📁 Ingestão de Dados de Averbacão em Lote (CSV)

| Script | `integracao_py_dbeaver_csv.py` |
| :--- | :--- |
| **Problema Curado** | Processar **múltiplos arquivos CSV** de averbação em uma pasta, onde a **primeira linha não é o cabeçalho** e contém metadados vitais (o período de autorização), exigindo extração, além da necessidade de lidar com a criação de uma tabela de destino. |
| **Solução Resolvida** | <ul><li>**Criação de Tabela:** Garante a estrutura correta no PostgreSQL com `CREATE TABLE IF NOT EXISTS`.</li><li>**Extração Inteligente de Data:** Uma função dedicada lê a primeira linha, usa **Expressões Regulares (`re`)** para isolar as datas de início e fim do período de autorização (que é crucial para a filtragem).</li><li>**Leitura Limpa:** O Pandas é instruído a **ignorar a primeira linha** (`skiprows=1`) ao ler o CSV de dados.</li><li>**Monitoramento:** A inserção é feita em **lotes menores** (`batch_size = 50`), permitindo um acompanhamento granular no DBeaver e melhor controle de *commits*.</li></ul> |

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Função Principal |
| :--- | :--- |
| **Python** | Linguagem principal para orquestração. |
| **Pandas** | Leitura (`.xlsx`, `.csv`) e manipulação/limpeza de DataFrames. |
| **Psycopg2** | Conexão e comunicação com o banco de dados PostgreSQL. |
| **`psycopg2.extras.execute_batch`** | Inserção de dados em lote para otimização de performance. |
| **PostgreSQL** | Banco de dados de destino. |
| **`datetime` / `re`** | Tratamento e conversão de datas / Extração de padrões (Regex). |

---

## 🚀 Como Executar

1.  **Instale as dependências necessárias:**
    ```bash
    pip install pandas psycopg2-binary
    ```
2.  **Configuração:**
    * **Credenciais:** Altere as variáveis de conexão (`host`, `port`, `dbname`, `user`, `password`) em ambos os scripts.
    * **Caminhos:** Ajuste os caminhos de origem dos arquivos/pastas (Ex: `r"c:\base_de_dados\..."`).
3.  **Execução:**
    ```bash
    python integracao_py_dbeaver_xlsx.py
    python integracao_py_dbeaver_csv.py
    ```
