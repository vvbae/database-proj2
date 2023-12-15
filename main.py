from datetime import date
from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
conn = psycopg2.connect("dbname=proj2 user=weizi")
cur = conn.cursor()

origins = [
    "http://localhost:3000",
    "localhost:3000"
]

app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
)

class User(BaseModel):
    name: str
    billing_addr: str

class Location(BaseModel):
    cid: int
    address: str
    unit_no: str
    zip_code: str
    start_date: str
    size_sqft: int
    num_beds: int
    num_occupants: int

class Device(BaseModel):
    lid: int
    time_added: str
    type: str
    model: str

async def run_query(query, fetch=False):
    cur.execute(query)
    conn.commit()
    if fetch:
        records = cur.fetchall()
        return records

# register new user
@app.post("/users/register")
async def register_user(user: User):
    q = f"""INSERT INTO Customer (name, billing_addr)
VALUES ('{user.name}', '{user.billing_addr}');"""
    ret = await run_query(q)
    return {"ret": ret}

# register new device
@app.post("/devices/register")
async def register_device(device: Device):
    q = f"""INSERT INTO Enroll_Device (lid, time_added, type, model)
VALUES ({device.lid}, '{device.time_added}', '{device.type}', '{device.model}');"""
    ret = await run_query(q)
    return {"ret": ret}

# register new location
@app.post("/locations/register")
async def register_location(location: Location):
    q = f"""INSERT INTO Service_Location (cid, address, unit_no, zip_code, 
start_date, size_sqft, num_beds, num_occupants)
VALUES ({location.cid}, '{location.address}', {location.unit_no}, 
'{location.zip_code}', '{location.start_date}', {location.size_sqft}, 
{location.num_beds}, {location.num_occupants});"""
    ret = await run_query(q)
    return {"ret": ret}

# remove device
@app.put("/devices/remove")
async def remove_device(device_id: int):
    q = f"""DELETE FROM Enroll_Device WHERE did = {device_id};"""
    ret = await run_query(q)
    return {"ret": ret}

# remove location
@app.put("/locations/remove")
async def remove_location(location_id: int):
    q = f"""DELETE FROM Service_Location WHERE lid = {location_id};"""
    ret = await run_query(q)
    return {"ret": ret}


# list user devices
@app.get("/devices")
async def list_devices(user_id: int):
    q = f"""
SELECT
    ed.did AS device_id,
    ed.type,
    ed.model,
    sl.address AS location_address
FROM
    Service_Location sl
JOIN
    Enroll_Device ed ON sl.lid = ed.lid
WHERE
    sl.cid = {user_id};
"""
    ret = await run_query(q, True)
    return {"Devices": ret}

# list user locations
@app.get("/locations")
async def list_locations(user_id: int):
    q = f"""SELECT * FROM Service_Location WHERE cid = {user_id};"""
    ret = await run_query(q, True)
    return {"Locations": ret}

# view daily energy consumption during a given month
@app.get("/views/1")
async def get_view_1(user_id: int, month: int, year: int):
    q = f"""
SELECT
    sl.cid AS user_id,
    CAST(de.timestamp AS DATE) AS date,
    SUM(de.value) AS total_energy_consumption
FROM
    Service_Location sl
JOIN
    Enroll_Device ed ON sl.lid = ed.lid
JOIN
    Device_Event de ON ed.did = de.did
WHERE
    sl.cid = {user_id}
    AND de.event_label = 'energy_use'
    AND EXTRACT(YEAR FROM de.timestamp) = '{year}'
    AND EXTRACT(MONTH FROM de.timestamp) = '{month}'
GROUP BY
    sl.cid,
    CAST(de.timestamp AS DATE)
    ;"""
    ret = await run_query(q, True)
    return {"ret": ret}

# view energy consumption per device for a given day
@app.get("/views/2")
async def get_view_2(user_id: int, day: date):
    q = f"""
SELECT
    ed.did AS device_id,
    de.timestamp AS datetime,
    de.value AS energy_consumption
FROM
    Service_Location sl
JOIN
    Enroll_Device ed ON sl.lid = ed.lid
JOIN
    Device_Event de ON ed.did = de.did
WHERE
    sl.cid = {user_id}
    AND de.event_label = 'energy_use'
    AND CAST(de.timestamp AS DATE) = '{day}'
GROUP BY
	ed.did,
    de.timestamp,
    de.value
;"""
    ret = await run_query(q, True)
    return {"ret": ret}

# view energy consumption during a given month compares to the average 
# consumption of other locations that are similar
@app.get("/views/3")
async def get_view_3(user_id: int, month: int, year: int):
    q = f"""
with x as (
    select 
    sl.cid, sl.lid, sl.size_sqft, de.value as cons
    from service_location sl
    join enroll_device ed on sl.lid = ed.lid
    join device_event de on ed.did = de.did
    where EXTRACT(YEAR FROM de.timestamp) = '{year}'
    AND EXTRACT(MONTH FROM de.timestamp) = '{month}'
    and de.event_label = 'energy_use'
    group by sl.cid, sl.lid, cons
)
,
sum_x as (
    select cid, lid, size_sqft, sum(cons) as cons from x
    group by cid, lid, size_sqft
),
sum_y as (
    select lid, size_sqft, sum(cons) as cons from x
    group by lid, size_sqft
)

select 
  sum_x.lid as curr_lid, sum_x.size_sqft, sum_x.cons as cons, avg(sum_y.cons) as avg_cons
from sum_x cross join sum_y
where sum_x.lid != sum_y.lid and sum_x.cid = {user_id}
and sum_y.size_sqft between sum_x.size_sqft * 0.95 and sum_x.size_sqft * 1.05
group by curr_lid, sum_x.size_sqft, sum_x.cons
    ;"""
    ret = await run_query(q, True)
    return {"ret": ret}

# view when devices were used at peak (high-price) times and how much 
# could have been saved by using them during a different time
@app.get("/views/4")
async def get_view_4(user_id: int, day: date):
    q = f"""
WITH PeakUsage AS (
    SELECT
  		sl.lid,
  		sl.zip_code,
        de.timestamp AS datetime,
        ep.price AS energy_price,
        sum(de.value) AS energy_consumption
    FROM
        Service_Location sl
    JOIN
        Enroll_Device ed ON sl.lid = ed.lid
    JOIN
        Device_Event de ON ed.did = de.did
    JOIN
        Energy_Price ep ON sl.zip_code = ep.zip_code and de.timestamp = ep.datetime
    WHERE
        sl.cid = {user_id}
  		AND de.event_label = 'energy_use'
  		and cast(de.timestamp as date) = '{day}'
  	group by sl.lid, sl.zip_code, de.timestamp, ep.price
),
Prices AS (
    SELECT *
    FROM
        Energy_Price
    WHERE
        zip_code in (
          select zip_code from PeakUsage
        )
  		and cast(datetime as date) = '{day}'
)

SELECT
    pu.datetime AS peak_time,
    pu.energy_price AS peak_energy_price,
    pu.energy_consumption AS peak_energy_consumption,
    opu.datetime AS off_peak_time,
    opu.price AS off_peak_energy_price,
    (pu.energy_price - opu.price) * pu.energy_consumption AS potential_savings
FROM
    PeakUsage pu
cross JOIN
    Prices opu where pu.energy_price > opu.price
;"""
    ret = await run_query(q, True)
    return {"ret": ret}

@app.get("/")
def read_root():
    return {"Hello": "World"}