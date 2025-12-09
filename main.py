from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import gallifrey
from datetime import datetime, timedelta
import re

app = FastAPI(
    title="Torre Automation API",
    description="API para automatizaciones de contacto con clientes por flags",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexión singleton
_poseidon = None

def get_poseidon():
    """Conexión única a Poseidon"""
    global _poseidon
    if _poseidon is None:
        _poseidon = gallifrey.database_factories.secret_manager_database_factory("poseidon")
    return _poseidon

def build_job_link(hash_id: str) -> str:
    """Construye el link de la vacante"""
    return f"https://torre.ai/post/{hash_id}"

def detect_industry(vacancy_name: str, company_name: str) -> str:
    """
    Detecta la industria basándose en palabras clave
    Esto se refinará más adelante según tus necesidades
    """
    text = f"{vacancy_name} {company_name}".lower()
    
    # Tech
    if any(word in text for word in ['developer', 'engineer', 'software', 'programmer', 'tech', 'data', 'cloud', 'devops', 'frontend', 'backend']):
        return "technology"
    
    # Sales/Marketing
    elif any(word in text for word in ['sales', 'marketing', 'account', 'business development', 'growth']):
        return "sales_marketing"
    
    # Finance
    elif any(word in text for word in ['finance', 'accountant', 'financial', 'controller', 'cfo']):
        return "finance"
    
    # Healthcare
    elif any(word in text for word in ['health', 'medical', 'doctor', 'nurse', 'clinical']):
        return "healthcare"
    
    # Design
    elif any(word in text for word in ['design', 'ux', 'ui', 'graphic', 'creative']):
        return "design"
    
    # HR/Operations
    elif any(word in text for word in ['human resources', 'hr', 'operations', 'admin']):
        return "operations_hr"
    
    # Customer Service
    elif any(word in text for word in ['customer', 'support', 'service', 'success']):
        return "customer_service"
    
    # Default
    else:
        return "general"

@app.get("/")
def health_check():
    """Endpoint de salud - para keep-alive de n8n"""
    return {
        "status": "running",
        "service": "Torre Automation API",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "available_flags": [
            "new-ts-posting",
            "less-than-6",
            "less-than-6-24h",
            "6-ruled-out",
            "less-than-6-reach",
            "new-reach",
            "no-activity"
        ]
    }

# ============================================
# FLAG 1: NEW TS POSTING (Prioridad ALTA)
# ============================================
@app.get("/flags/new-ts-posting")
def get_new_ts_posting(days: int = Query(7, description="Días hacia atrás")):
    """
    Flag: New TS Posting
    TSs publicando por PRIMERA VEZ en los últimos X días
    
    Condición: published_date = poster_first_post
    Incluye: TODAS (approved Y unapproved)
    Excluye: business_line = 'torre_os'
    Solo: status = 'open'
    
    Retorna:
    - Nombre del TS
    - Email
    - Nombre de la vacante
    - Link de la vacante
    - Idioma (locale)
    - Fecha de publicación
    - Nombre de la compañía
    - Industria detectada (para personalización)
    """
    try:
        poseidon = get_poseidon()
        
        query = f"""
        SELECT DISTINCT
            mg.name AS ts_name,
            mg.email AS ts_email,
            mj.objective AS vacancy_name,
            mj.hash_id AS hash_id,
            mj.locale AS locale,
            mj.published_date AS published_date,
            mj.organization_name AS company_name,
            mj.poster_gg_id AS poster_gg_id,
            mj.review AS review_status
        FROM poseidon.mart_jobs mj
        INNER JOIN poseidon.mart_genomes mg ON mj.poster_gg_id = mg.gg_id
        WHERE mj.published_date = mj.poster_first_post
            AND mj.status = 'open'
            AND mj.published_date >= current_date - INTERVAL '{days} days'
            AND mj.published_date < current_date + INTERVAL '1 day'
            AND (mj.business_line <> 'torre_os' OR mj.business_line IS NULL)
        ORDER BY mj.published_date DESC;
        """
        
        results = poseidon.execute_query(query)
        
        data = []
        for r in results:
            vacancy_name = r[2] if r[2] else "Untitled Position"
            company_name = r[6] if r[6] else "Company"
            
            data.append({
                "ts_name": r[0],
                "ts_email": r[1],
                "vacancy_name": vacancy_name,
                "vacancy_link": build_job_link(r[3]),
                "locale": r[4],
                "published_date": str(r[5]),
                "company_name": company_name,
                "poster_gg_id": r[7],
                "review_status": r[8],
                "industry": detect_industry(vacancy_name, company_name),
                "flag": "new_ts_posting"
            })
        
        return {
            "success": True,
            "flag": "new_ts_posting",
            "description": "TSs posting for the first time (includes approved & unapproved)",
            "count": len(data),
            "days_lookback": days,
            "data": data
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================
# FLAG 2: LESS THAN 6
# ============================================
@app.get("/flags/less-than-6")
def get_less_than_6(days: int = Query(30, description="Días hacia atrás")):
    """
    Flag: Less than 6
    Jobs APROBADOS con menos de 6 aplicantes valiosos
    
    Solo: review = 'approved'
    Solo: status = 'open'
    Excluye: business_line = 'torre_os'
    """
    try:
        poseidon = get_poseidon()
        
        query = f"""
        SELECT DISTINCT
            mg.name AS ts_name,
            mg.email AS ts_email,
            mj.objective AS vacancy_name,
            mj.hash_id AS hash_id,
            mj.locale AS locale,
            mj.published_date AS published_date,
            mj.organization_name AS company_name,
            mj.poster_gg_id AS poster_gg_id,
            mj.valuable_appls AS valuable_appls
        FROM poseidon.mart_jobs mj
        INNER JOIN poseidon.mart_genomes mg ON mj.poster_gg_id = mg.gg_id
        WHERE mj.review = 'approved'
            AND mj.status = 'open'
            AND mj.valuable_appls < 6
            AND mj.published_date >= current_date - INTERVAL '{days} days'
            AND mj.published_date < current_date + INTERVAL '1 day'
            AND (mj.business_line <> 'torre_os' OR mj.business_line IS NULL)
        ORDER BY mj.published_date DESC;
        """
        
        results = poseidon.execute_query(query)
        
        data = []
        for r in results:
            vacancy_name = r[2] if r[2] else "Untitled Position"
            company_name = r[6] if r[6] else "Company"
            
            data.append({
                "ts_name": r[0],
                "ts_email": r[1],
                "vacancy_name": vacancy_name,
                "vacancy_link": build_job_link(r[3]),
                "locale": r[4],
                "published_date": str(r[5]),
                "company_name": company_name,
                "poster_gg_id": r[7],
                "valuable_appls": r[8],
                "industry": detect_industry(vacancy_name, company_name),
                "flag": "less_than_6"
            })
        
        return {
            "success": True,
            "flag": "less_than_6",
            "description": "Approved openings with less than 6 relevant applicants",
            "count": len(data),
            "days_lookback": days,
            "data": data
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================
# FLAG 3: NO ACTIVITY (Prioridad ALTA)
# ============================================
@app.get("/flags/no-activity")
def get_no_activity(
    inactive_days: int = Query(7, description="Días sin actividad"),
    lookback_days: int = Query(30, description="Días hacia atrás para publicación")
):
    """
    Flag: No Activity
    Jobs APROBADOS sin actividad en el pipeline por más de X días
    
    Solo: review = 'approved'
    Solo: status = 'open'
    Excluye: business_line = 'torre_os'
    
    Nota: Esta query puede tardar más
    """
    try:
        poseidon = get_poseidon()
        
        query = f"""
        SELECT DISTINCT
            mg.name AS ts_name,
            mg.email AS ts_email,
            mj.objective AS vacancy_name,
            mj.hash_id AS hash_id,
            mj.locale AS locale,
            mj.published_date AS published_date,
            mj.organization_name AS company_name,
            mj.poster_gg_id AS poster_gg_id
        FROM poseidon.mart_jobs mj
        INNER JOIN poseidon.mart_genomes mg ON mj.poster_gg_id = mg.gg_id
        LEFT JOIN (
            SELECT 
                ma.opportunity_id,
                MAX(GREATEST(
                    COALESCE(ma.disqualified_date, '1970-01-01'::timestamp),
                    COALESCE(ma.mm_date, '1970-01-01'::timestamp)
                )) AS last_activity
            FROM poseidon.mart_applications ma
            GROUP BY ma.opportunity_id
        ) AS activity ON mj.opportunity_id = activity.opportunity_id
        WHERE mj.review = 'approved'
            AND mj.status = 'open'
            AND mj.published_date >= current_date - INTERVAL '{lookback_days} days'
            AND (mj.business_line <> 'torre_os' OR mj.business_line IS NULL)
            AND (
                activity.last_activity < current_date - INTERVAL '{inactive_days} days'
                OR activity.last_activity IS NULL
            )
        ORDER BY mj.published_date DESC;
        """
        
        results = poseidon.execute_query(query)
        
        data = []
        for r in results:
            vacancy_name = r[2] if r[2] else "Untitled Position"
            company_name = r[6] if r[6] else "Company"
            
            data.append({
                "ts_name": r[0],
                "ts_email": r[1],
                "vacancy_name": vacancy_name,
                "vacancy_link": build_job_link(r[3]),
                "locale": r[4],
                "published_date": str(r[5]),
                "company_name": company_name,
                "poster_gg_id": r[7],
                "industry": detect_industry(vacancy_name, company_name),
                "flag": "no_activity"
            })
        
        return {
            "success": True,
            "flag": "no_activity",
            "description": f"Approved jobs with no activity in pipeline for {inactive_days}+ days",
            "count": len(data),
            "inactive_days_threshold": inactive_days,
            "lookback_days": lookback_days,
            "data": data
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================
# ENDPOINT MULTI-FLAG (Para queries complejas)
# ============================================
@app.get("/flags/all-priorities")
def get_all_high_priority_flags(days: int = Query(7, description="Días hacia atrás")):
    """
    Obtiene TODAS las flags de prioridad ALTA en una sola llamada
    
    Retorna:
    - new_ts_posting
    - less_than_6
    - no_activity
    
    Útil para dashboard o vista consolidada
    """
    try:
        results = {
            "success": True,
            "flags": {}
        }
        
        # Flag 1: New TS Posting
        new_ts = get_new_ts_posting(days)
        results["flags"]["new_ts_posting"] = new_ts
        
        # Flag 2: Less than 6
        less_6 = get_less_than_6(days)
        results["flags"]["less_than_6"] = less_6
        
        # Flag 3: No Activity
        no_activity = get_no_activity(inactive_days=7, lookback_days=days)
        results["flags"]["no_activity"] = no_activity
        
        # Resumen
        total_count = (
            new_ts.get("count", 0) + 
            less_6.get("count", 0) + 
            no_activity.get("count", 0)
        )
        
        results["summary"] = {
            "total_clients_to_contact": total_count,
            "breakdown": {
                "new_ts_posting": new_ts.get("count", 0),
                "less_than_6": less_6.get("count", 0),
                "no_activity": no_activity.get("count", 0)
            }
        }
        
        return results
    
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================
# ENDPOINT PARA DETALLES DE UN CLIENTE
# ============================================
@app.get("/client/{email}")
def get_client_details(email: str):
    """
    Obtiene todos los detalles de un TS específico por email
    Útil para personalización adicional
    """
    try:
        poseidon = get_poseidon()
        
        query = f"""
        SELECT 
            mg.name,
            mg.email,
            mg.gg_id,
            COUNT(DISTINCT mj.hash_id) AS total_jobs,
            COUNT(DISTINCT CASE WHEN mj.status = 'open' THEN mj.hash_id END) AS open_jobs,
            MIN(mj.published_date) AS first_job_date,
            MAX(mj.published_date) AS last_job_date
        FROM poseidon.mart_genomes mg
        LEFT JOIN poseidon.mart_jobs mj ON mg.gg_id = mj.poster_gg_id
        WHERE mg.email = '{email}'
        GROUP BY mg.name, mg.email, mg.gg_id;
        """
        
        results = poseidon.execute_query(query)
        
        if len(results) == 0:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        r = results[0]
        return {
            "success": True,
            "data": {
                "name": r[0],
                "email": r[1],
                "gg_id": r[2],
                "total_jobs_posted": r[3],
                "open_jobs": r[4],
                "first_job_date": str(r[5]) if r[5] else None,
                "last_job_date": str(r[6]) if r[6] else None
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================
# ENDPOINT PARA OBTENER INDUSTRIAS DISPONIBLES
# ============================================
@app.get("/industries")
def get_available_industries():
    """
    Retorna la lista de industrias detectables
    Útil para configurar templates de mensajes en n8n
    """
    return {
        "success": True,
        "industries": {
            "technology": {
                "name": "Technology",
                "keywords": ["developer", "engineer", "software", "tech", "data", "cloud"]
            },
            "sales_marketing": {
                "name": "Sales & Marketing",
                "keywords": ["sales", "marketing", "account", "growth"]
            },
            "finance": {
                "name": "Finance",
                "keywords": ["finance", "accountant", "financial", "cfo"]
            },
            "healthcare": {
                "name": "Healthcare",
                "keywords": ["health", "medical", "doctor", "nurse"]
            },
            "design": {
                "name": "Design",
                "keywords": ["design", "ux", "ui", "graphic", "creative"]
            },
            "operations_hr": {
                "name": "Operations & HR",
                "keywords": ["hr", "operations", "admin"]
            },
            "customer_service": {
                "name": "Customer Service",
                "keywords": ["customer", "support", "service"]
            },
            "general": {
                "name": "General",
                "keywords": []
            }
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)