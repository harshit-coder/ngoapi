import uvicorn
from app import app

if __name__ == "__main__":
    uvicorn.run(app, host='3.108.254.233', port=8005)
