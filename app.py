import os

from fastapi import FastAPI, BackgroundTasks
from fastapi.templating import Jinja2Templates
from selenium import webdriver

from database import db_connect
from models import collect_state

app = FastAPI()
templates = Jinja2Templates(directory="templates")
connect = db_connect()

@app.get("/scrape")
async def home(background_tasks: BackgroundTasks):
    background_tasks.add_task(collect_state_publisher)
    return {"message": "success"}


#@app.get("/get_excel")
#def scrape():



def collect_state_publisher():
    collect_state.delay()


#@app.get("/check_status")
#def check_status():



