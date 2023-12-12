from datetime import date, datetime
from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI()
conn = psycopg2.connect("dbname=proj2 user=weizi")
cur = conn.cursor()

class User(BaseModel):
    name: str
    billing_addr: str

class Location(BaseModel):
    cid: int
    address: str
    unit_no: str
    zip_code: str
    start_date: datetime
    size_sqft: int
    num_beds: int
    num_occupants: int

class Device(BaseModel):
    lid: int
    time_added: datetime
    type: str
    model: str

def run_query(query):
    cur.execute(query)
    conn.commit()
    records = cur.fetchall()
    return records

# register new user
@app.put("/users/register")
def register_user(user: User):
    q = f"""INSERT INTO Customer (name, billing_addr)
VALUES (${user.name}, ${user.billing_addr});"""
    ret = run_query(q)
    return {"New customer registered!"}

# register new device
@app.put("/devices/register")
def register_device(device: Device):
    q = f"""INSERT INTO Enroll_Device (lid, time_added, type, model)
VALUES (${device.lid}, ${device.time_added}, ${device.type}, ${device.model});"""
    ret = run_query(q)
    return {"New device registered!"}

# register new location
@app.put("/locations/register")
def register_location(location: Location):
    q = f"""INSERT INTO Service_Location (cid, address, unit_no, zip_code, 
start_date, size_sqft, num_beds, num_occupants)
VALUES (${location.cid}, ${location.address}, ${location.unit_no}, 
${location.zip_code}, ${location.start_date}, ${location.size_sqft}, 
${location.num_beds}, ${location.num_occupants});"""
    ret = run_query(q)
    return {"New location registered!"}

# remove device
@app.put("/devices/remove")
def remove_device(device_id: int):
    q = f"""DELETE FROM Enroll_Device WHERE did = ${device_id};"""
    ret = run_query(q)
    return {"Device removed."}

# remove location
@app.put("/locations/remove")
def remove_location(location_id: int):
    q = f"""DELETE FROM Service_Location WHERE lid = ${location_id};"""
    ret = run_query(q)
    lid += 1
    return {"Location removed."}


# list user devices
@app.get("/devices/{user_id}")
def list_devices(user_id: int):
    q = f"""SELECT * FROM Enroll_Device WHERE cid = ${user_id};"""
    ret = run_query(q)
    return {"Devices": ret}

# list user locations
@app.get("/locations/{user_id}")
def list_locations(user_id: int):
    q = f"""SELECT * FROM Service_Location WHERE cid = ${user_id};"""
    ret = run_query(q)
    return {"Locations": ret}

# view daily energy consumption during a given day
@app.get("/views/1")
def list_locations(user_id: int, day: date):
    # TODO:
    q = f""";"""
    ret = run_query(q)
    return {"ret": ret}

# view energy consumption per device for a given day
@app.get("/views/2")
def list_locations(device_id: int, day: date):
    # TODO:
    q = f""";"""
    ret = run_query(q)
    return {"ret": ret}

# view energy consumption during a given day compares to the average 
# consumption of other locations that are similar
@app.get("/views/3")
def list_locations(user_id: int, day: date):
    # TODO:
    q = f""";"""
    ret = run_query(q)
    return {"ret": ret}

# view when devices were used at peak (high-price) times and how much 
# could have been saved by using them during a different time
@app.get("/views/4")
def list_locations(user_id: int):
    # TODO:
    q = f""";"""
    ret = run_query(q)
    return {"ret": ret}

@app.get("/")
def read_root():
    return {"Hello": "World"}