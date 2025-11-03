# from fastapi import APIRouter, HTTPException, Body
# from libs.utils import load_pw_domains, add_pw_domain, remove_pw_domain

# router = APIRouter(prefix="/policy", tags=["policy"])

# @router.get("/playwright-domains")
# def list_pw_domains():
#     domains = sorted(load_pw_domains())
#     return {"count": len(domains), "domains": domains}

# @router.post("/playwright-domains")
# def add_pw_domain_api(domain: str = Body(..., embed=True)):
#     if not domain:
#         raise HTTPException(status_code=400, detail="domain required")
#     add_pw_domain(domain)
#     return {"ok": True, "domain": domain}

# @router.delete("/playwright-domains/{domain}")
# def del_pw_domain_api(domain: str):
#     if not domain:
#         raise HTTPException(status_code=400, detail="domain required")
#     remove_pw_domain(domain)
#     return {"ok": True, "domain": domain}
