"""
Exercício Prático 01 — Coletor de Dados da OpenF1 para MongoDB
Objetivo: Coletar dados da API OpenF1 (sessões, pilotos e voltas) e armazená-los
          no MongoDB de forma modular, configurável e idempotente.
"""

import os
import sys
import logging
from typing import Any, Dict, List, Optional
import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# ========================================================
# 1. Configurações e Variáveis de Ambiente
# ========================================================
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Configuração da conexão com MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME_F1", "openf1_data")

# URL base da API OpenF1
API_BASE_URL = os.getenv("OPENF1_BASE_URL", "https://api.openf1.org/v1")

# Parâmetros configuráveis da sessão de demonstração
# Monza 2023: meeting_key=1219, session_key=9159
SESSION_KEY = int(os.getenv("F1_SESSION_KEY", "9159"))
MEETING_KEY = int(os.getenv("F1_MEETING_KEY", "1219"))


# ========================================================
# 2. Conexão com o MongoDB
# ========================================================
def get_mongo_connection():
    """
    Estabelece a conexão com o MongoDB utilizando a URI configurada
    e retorna o objeto do banco de dados (db).

    Returns:
        Database: Instância do banco de dados MongoDB (openf1_data).
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        # Teste de conexão imediato
        client.admin.command("ping")
        db = client[MONGO_DB_NAME]
        logging.info("Conectado com sucesso ao MongoDB no banco '%s'.", MONGO_DB_NAME)
        return db
    except Exception as e:
        logging.exception("Erro ao conectar ao MongoDB (%s): %s", MONGO_URI, e)
        raise


# ========================================================
# 3. Busca de Dados da API OpenF1
# ========================================================
def fetch_data(endpoint: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Realiza requisição GET em um endpoint da API OpenF1.

    Args:
        endpoint (str): Nome do endpoint (ex: 'sessions', 'drivers', 'laps').
        params (dict, optional): Parâmetros de consulta (query string).

    Returns:
        list: Lista de dicionários contendo os registros retornados pela API.
    """
    url = f"{API_BASE_URL}/{endpoint}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "OpenF1-Collector/1.0",
    }

    try:
        logging.info("Buscando dados em '%s' com parâmetros: %s", endpoint, params)
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        if isinstance(data, list):
            logging.info("%d registros obtidos com sucesso do endpoint '%s'.", len(data), endpoint)
            return data
        elif isinstance(data, dict):
            logging.info("1 registro obtido do endpoint '%s'.", endpoint)
            return [data]
        else:
            logging.warning("Formato inesperado retornado por '%s': %s", endpoint, type(data))
            return []

    except requests.RequestException as e:
        logging.error("Falha ao buscar dados do endpoint '%s': %s", endpoint, e)
        return []


# ========================================================
# 4. Armazenamento Idempotente no MongoDB
# ========================================================
def save_to_collection(db, data: List[Dict[str, Any]], collection_name: str, unique_keys: List[str]) -> int:
    """
    Salva ou atualiza registros em uma coleção do MongoDB garantindo idempotência
    através de update_one(..., upsert=True) baseado nas chaves únicas especificadas.

    Regras de chaves únicas:
      - sessions: ['session_key']
      - drivers:  ['session_key', 'driver_number']
      - laps:     ['session_key', 'driver_number', 'lap_number']

    Args:
        db: Objeto de banco de dados do MongoDB.
        data (list): Lista de documentos a serem persistidos.
        collection_name (str): Nome da coleção de destino.
        unique_keys (list): Lista de atributos que compõem a chave composta/única.

    Returns:
        int: Quantidade de registros gravados com sucesso.
    """
    if not data:
        logging.warning("Nenhum dado fornecido para gravar na coleção '%s'.", collection_name)
        return 0

    collection = db[collection_name]
    processados = 0
    ignorados = 0

    logging.info("Iniciando gravação de %d registros na coleção '%s'...", len(data), collection_name)

    for record in data:
        # Monta a query de filtro baseada nas chaves únicas
        query = {key: record.get(key) for key in unique_keys}

        # Ignora se algum campo chave obrigatório estiver ausente (None)
        if any(v is None for v in query.values()):
            logging.warning("Registro ignorado por chave única incompleta (%s): %s", query, record)
            ignorados += 1
            continue

        try:
            # Operação idempotente com upsert=True
            collection.update_one(query, {"$set": record}, upsert=True)
            processados += 1
        except PyMongoError as e:
            logging.error("Erro ao persistir documento com chave %s na coleção '%s': %s", query, collection_name, e)

    logging.info(
        "Finalizada gravação na coleção '%s': %d processados, %d ignorados.",
        collection_name,
        processados,
        ignorados,
    )
    return processados


# ========================================================
# 5. Fluxo Principal de Execução
# ========================================================
def main() -> int:
    """
    Coordena a execução do coletor:
    1. Conecta ao MongoDB.
    2. Coleta dados da sessão específica (Monza 2023 - session_key=9159).
    3. Coleta dados dos pilotos da sessão.
    4. Coleta dados das voltas da sessão.
    5. Grava cada conjunto na respectiva coleção.
    """
    logging.info("Iniciando pipeline de coleta OpenF1...")

    try:
        db = get_mongo_connection()

        # ---- Passo 1: Coletar dados da sessão específica ----
        logging.info("--- Passo 1: Sessão (session_key=%d) ---", SESSION_KEY)
        sessions_data = fetch_data("sessions", {"session_key": SESSION_KEY})
        save_to_collection(db, sessions_data, "sessions", ["session_key"])

        # ---- Passo 2: Coletar pilotos da sessão ----
        logging.info("--- Passo 2: Pilotos da sessão %d ---", SESSION_KEY)
        drivers_data = fetch_data("drivers", {"session_key": SESSION_KEY})
        save_to_collection(db, drivers_data, "drivers", ["session_key", "driver_number"])

        # ---- Passo 3: Coletar voltas da sessão ----
        logging.info("--- Passo 3: Voltas da sessão %d ---", SESSION_KEY)
        laps_data = fetch_data("laps", {"session_key": SESSION_KEY})
        save_to_collection(db, laps_data, "laps", ["session_key", "driver_number", "lap_number"])

        logging.info("Coleta OpenF1 finalizada com sucesso!")
        return 0

    except Exception as e:
        logging.critical("Execução do coletor OpenF1 falhou: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
