# Projetos de Ingestão de Dados com MongoDB

Este repositório contém dois pipelines ETL em Python para ingestão de dados em bancos de dados MongoDB NoSQL:
1. **Cartola FC ETL** (`cartola_etl.py`): Coleta de atletas, clubes e status de mercado da API do Cartola FC.
2. **OpenF1 Collector** (`f1_data_collector.py`): Coleta de sessões, pilotos e voltas da API OpenF1 (GP de Monza 2023).

---

## 📁 Estrutura dos Arquivos

- [`cartola_etl.py`](cartola_etl.py): Pipeline de extração, transformação e carga do Cartola FC.
- [`f1_data_collector.py`](f1_data_collector.py): Pipeline idempotente de coleta da OpenF1.
- [`f1_data_coletor.py`](f1_data_coletor.py): Script ponte para compatibilidade.
- [`.env.example`](.env.example): Modelo com as variáveis de ambiente necessárias.
- [`requirements.txt`](requirements.txt): Dependências do projeto (`requests`, `pymongo`, `python-dotenv`).

---

## 🚀 Como Executar

### 1. Criar e ativar o ambiente virtual (opcional, mas recomendado)

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux / Mac
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Instalar as dependências

```bash
pip install -r requirements.txt
```

### 3. Configurar o arquivo `.env`

Crie uma cópia de `.env.example` nomeada como `.env`:

```bash
# Windows (PowerShell)
Copy-Item .env.example .env

# Linux / Mac
cp .env.example .env
```

Abra o arquivo `.env` e configure a sua string de conexão `MONGO_URI` (MongoDB Local ou MongoDB Atlas).

---

### 4. Executar os Scripts

#### ⚽ Exercício 2 — Cartola FC:
Grava no banco `cartola_fc_db` nas coleções:
- `clubes_rodada_atual` (upsert por `_id`)
- `atletas_rodada_atual` (limpa e insere lista atual)
- `mercado_rodada_atual` (mantém status mais recente)

```bash
python cartola_etl.py
```

#### 🏎️ Exercício 1 — OpenF1:
Grava no banco `openf1_data` nas coleções:
- `sessions` (chave única: `session_key`)
- `drivers` (chave única: `session_key` + `driver_number`)
- `laps` (chave única: `session_key` + `driver_number` + `lap_number`)

```bash
python f1_data_collector.py
```
