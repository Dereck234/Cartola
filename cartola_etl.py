"""
ETL Cartola FC para MongoDB
Projeto: Coleta e Armazenamento de Dados do Cartola FC
Objetivo: Automatizar a extração de dados da API do Cartola FC (mercado, atletas e clubes)
          e persistência estruturada no MongoDB.
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# ========================================================
# Configuração de logging
# ========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
# Força timestamps em UTC no logger
logging.Formatter.converter = time.gmtime

# ========================================================
# Constantes e configuração
# ========================================================
API_BASE_URL = "https://api.cartola.globo.com"
MERCADO_ENDPOINT = f"{API_BASE_URL}/atletas/mercado"
STATUS_MERCADO_ENDPOINT = f"{API_BASE_URL}/mercado/status"

load_dotenv()  # Lê variáveis do arquivo .env, se existir

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "cartola_fc_db")

# Nomes das coleções conforme especificação do trabalho
COL_CLUBES = "clubes_rodada_atual"
COL_ATLETAS = "atletas_rodada_atual"
COL_MERCADO = "mercado_rodada_atual"


# ========================================================
# Utilidades
# ========================================================
def iso_utc_now() -> str:
    """Retorna timestamp ISO-8601 em UTC (ex: 2026-09-28T18:00:00Z)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ========================================================
# Módulo de Conexão
# ========================================================
def conectar_mongodb():
    """
    Lê as credenciais do ambiente e estabelece conexão com o MongoDB.
    
    Returns:
        Database: Objeto de banco de dados do MongoDB (db).
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        # Executa comando ping para validar imediatamente a conectividade
        client.admin.command("ping")
        db = client[MONGO_DB_NAME]
        logging.info("Conexão com o MongoDB estabelecida com sucesso no banco '%s'.", MONGO_DB_NAME)
        return db
    except Exception as e:
        logging.exception("Falha ao conectar no MongoDB: %s", e)
        raise


# ========================================================
# Módulo de Extração
# ========================================================
def buscar_dados_mercado(session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """
    Faz requisição GET para a API do Cartola FC para obter dados de mercado e atletas.
    Também consulta o endpoint complementar de status de mercado para garantir
    a integridade das informações de rodada, status e fechamento.

    Args:
        session: requests.Session opcional para reutilização de conexão.

    Returns:
        Dict com os dados decodificados do JSON da API.
    """
    sess = session or requests.Session()
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    # 1. Requisição principal: atletas, clubes e posições
    max_tentativas = 3
    dados_mercado = {}
    for tentativa in range(1, max_tentativas + 1):
        try:
            resp = sess.get(MERCADO_ENDPOINT, headers=headers, timeout=30)
            resp.raise_for_status()
            dados_mercado = resp.json()
            logging.info("Dados de atletas e clubes obtidos com sucesso da API.")
            break
        except requests.RequestException as e:
            logging.warning("Erro na requisição de atletas/mercado (tentativa %d/%d): %s", tentativa, max_tentativas, e)
            if tentativa == max_tentativas:
                logging.exception("Falha ao buscar dados do mercado após %d tentativas.", max_tentativas)
                raise
            time.sleep(2 ** tentativa)  # Backoff exponencial: 2s, 4s

    # 2. Requisição complementar: status do mercado (rodada_atual, status_mercado, fechamento)
    try:
        resp_status = sess.get(STATUS_MERCADO_ENDPOINT, headers=headers, timeout=15)
        if resp_status.status_code == 200:
            dados_mercado["status_mercado_info"] = resp_status.json()
            logging.info("Informações complementares de status do mercado obtidas com sucesso.")
        else:
            logging.warning("Não foi possível obter status detalhado do mercado (HTTP %d).", resp_status.status_code)
    except requests.RequestException as e:
        logging.warning("Aviso: Falha ao consultar endpoint de status do mercado: %s", e)

    return dados_mercado


# ========================================================
# Módulo de Transformação e Carga (ETL Core)
# ========================================================
def processar_e_gravar_dados(db, dados_mercado: Dict[str, Any]) -> None:
    """
    Processa os dados recebidos da API e grava nas coleções do MongoDB:
    1. clubes_rodada_atual: upsert por _id para evitar duplicatas.
    2. atletas_rodada_atual: limpa a coleção e insere a lista completa atualizada com timestamp.
    3. mercado_rodada_atual: mantém apenas o documento mais recente com timestamp.

    Args:
        db: Objeto de banco de dados do MongoDB.
        dados_mercado: Dicionário com os dados brutos obtidos da API.
    """
    timestamp = iso_utc_now()

    # ----------------------------------------------------
    # 1. Dados dos Clubes
    # ----------------------------------------------------
    logging.info("Gravando dados dos clubes...")
    clubes_obj = dados_mercado.get("clubes", {})
    if isinstance(clubes_obj, dict):
        ops: List[UpdateOne] = []
        for club_id_str, club_data in clubes_obj.items():
            try:
                club_id = int(club_id_str)
            except (TypeError, ValueError):
                club_id = club_data.get("id")
                if club_id is None:
                    logging.warning("Clube com ID inválido ignorado: %s", club_data)
                    continue

            # Formata o documento do clube conforme o modelo especificado
            doc_clube = {
                "_id": club_id,
                "nome": club_data.get("nome"),
                "abreviacao": club_data.get("abreviacao"),
                "escudos": club_data.get("escudos"),
                "nome_fantasia": club_data.get("nome_fantasia"),
                "timestamp_coleta": timestamp,
            }

            ops.append(
                UpdateOne(
                    {"_id": doc_clube["_id"]},
                    {"$set": doc_clube},
                    upsert=True,
                )
            )

        if ops:
            try:
                result = db[COL_CLUBES].bulk_write(ops, ordered=False)
                upserts = result.upserted_count or 0
                modified = result.modified_count or 0
                logging.info(
                    "Coleção '%s' atualizada: %d inseridos/upsert, %d atualizados.",
                    COL_CLUBES,
                    upserts,
                    modified,
                )
            except PyMongoError as e:
                logging.exception("Erro ao gravar clubes no MongoDB: %s", e)
                raise
    else:
        logging.warning("Formato inesperado para o objeto 'clubes'.")

    # ----------------------------------------------------
    # 2. Dados dos Atletas
    # ----------------------------------------------------
    logging.info("Gravando dados dos atletas...")
    atletas_list = dados_mercado.get("atletas", [])
    if not isinstance(atletas_list, list):
        logging.warning("Campo 'atletas' não é uma lista. Tipo recebido: %s", type(atletas_list))
        atletas_list = []

    # Adiciona timestamp_coleta em cada atleta
    for atleta in atletas_list:
        atleta["timestamp_coleta"] = timestamp

    try:
        # Garante que apenas a última consulta estará presente
        del_res = db[COL_ATLETAS].delete_many({})
        logging.info("Removidos %d registros anteriores da coleção '%s'.", del_res.deleted_count, COL_ATLETAS)

        if atletas_list:
            db[COL_ATLETAS].insert_many(atletas_list, ordered=False)
            logging.info("Inseridos %d atletas na coleção '%s'.", len(atletas_list), COL_ATLETAS)
        else:
            logging.info("Nenhum atleta encontrado para inserção.")
    except PyMongoError as e:
        logging.exception("Erro ao gravar atletas no MongoDB: %s", e)
        raise

    # ----------------------------------------------------
    # 3. Dados do Status do Mercado
    # ----------------------------------------------------
    logging.info("Gravando dados do mercado...")
    status_mercado_info = dados_mercado.get("status_mercado_info", {})

    mercado_doc = {
        "rodada_atual": status_mercado_info.get("rodada_atual", dados_mercado.get("rodada_atual")),
        "status_mercado": status_mercado_info.get("status_mercado", dados_mercado.get("status_mercado")),
        "aviso": status_mercado_info.get("aviso", dados_mercado.get("aviso", "")),
        "fechamento": status_mercado_info.get("fechamento", dados_mercado.get("fechamento")),
        "timestamp_coleta": timestamp,
    }

    try:
        # Mantém apenas o status mais recente
        db[COL_MERCADO].delete_many({})
        db[COL_MERCADO].insert_one(mercado_doc)
        logging.info("Status do mercado gravado na coleção '%s'.", COL_MERCADO)
    except PyMongoError as e:
        logging.exception("Erro ao gravar status do mercado no MongoDB: %s", e)
        raise


# ========================================================
# Orquestração (main)
# ========================================================
def main() -> int:
    """Função principal de orquestração do pipeline ETL."""
    logging.info("Iniciando processo ETL do Cartola FC...")
    try:
        logging.info("Conectando ao MongoDB...")
        db = conectar_mongodb()

        logging.info("Buscando dados na API...")
        dados = buscar_dados_mercado()

        logging.info("Processando e gravando dados no MongoDB...")
        processar_e_gravar_dados(db, dados)

        logging.info("Finalizado com sucesso.")
        return 0
    except Exception as e:
        logging.error("Execução do ETL interrompida por erro: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
