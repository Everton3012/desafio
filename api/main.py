# api/main.py
import os
os.environ['PGCLIENTENCODING'] = 'UTF8'

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

from api.db import get_conn, get_cursor
from api.schemas import OperadoraListResponse, EstatisticasResponse
from api import queries

app = FastAPI(
    title="IntuitiveCare - Teste Técnico", 
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "API IntuitiveCare", "version": "1.0.0"}


@app.get("/health")
def health_check():
    try:
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


@app.get("/api/operadoras", response_model=OperadoraListResponse)
def list_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    q: str | None = Query(None),
):
    try:
        offset = (page - 1) * limit

        with get_conn() as conn:
            with get_cursor(conn) as cur:
                if q and q.strip():
                    q_like = f"%{q.strip()}%"
                    cur.execute(queries.Q_OPERADORAS_COUNT_FILTER, {"q_like": q_like})
                    total = cur.fetchone()["total"]
                    cur.execute(queries.Q_OPERADORAS_LIST_FILTER, {"q_like": q_like, "limit": limit, "offset": offset})
                    rows = cur.fetchall()
                else:
                    cur.execute(queries.Q_OPERADORAS_COUNT_ALL)
                    total = cur.fetchone()["total"]
                    cur.execute(queries.Q_OPERADORAS_LIST_ALL, {"limit": limit, "offset": offset})
                    rows = cur.fetchall()

        return {"data": rows, "total": total, "page": page, "limit": limit}

    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/operadoras/{cnpj}")
def get_operadora(cnpj: str):
    try:
        cnpj = "".join([c for c in cnpj if c.isdigit()])
        
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute(queries.Q_OPERADORA_DETAIL, {"cnpj": cnpj})
                row = cur.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Operadora não encontrada")
        
        return row
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/operadoras/{cnpj}/despesas")
def get_despesas_operadora(cnpj: str):
    try:
        cnpj = "".join([c for c in cnpj if c.isdigit()])
        
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute(queries.Q_OPERADORA_DESPESAS, {"cnpj": cnpj})
                rows = cur.fetchall()
        
        return {"cnpj": cnpj, "despesas": rows}
        
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/estatisticas", response_model=EstatisticasResponse)
def get_estatisticas():
    try:
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute(queries.Q_ESTATS)
                stats = cur.fetchone()
                
                cur.execute(queries.Q_TOP5)
                top5 = cur.fetchall()
                
                cur.execute(queries.Q_UF_TOP5)
                topuf = cur.fetchall()

        return {
            "total_despesas": float(stats["total"]) if stats else 0,
            "media_despesas": float(stats["media"]) if stats else 0,
            "top_5_operadoras": top5 or [],
            "despesas_por_uf_top5": topuf or [],
        }
        
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))