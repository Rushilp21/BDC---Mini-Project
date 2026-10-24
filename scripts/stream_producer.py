"""CLI uses the same bounded producer as the dashboard."""
import os
import httpx
if __name__ == '__main__':
    r = httpx.post(os.getenv('BACKEND_URL', 'http://localhost:8000') + '/api/simulation/start',
                   headers={'X-Admin-Token': os.getenv('ADMIN_TOKEN', 'local-demo-admin')})
    r.raise_for_status()
    print(r.json())
