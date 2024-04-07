import requests
import time
from concurrent.futures import ThreadPoolExecutor

STATIONS_URL = 'https://wegfinder.at/api/v1/stations'


def fetch_data(url:str, max_retry=5) -> list[dict]:
    """Get API data"""
    retries = 0
    response = requests.get(url)

    while retries < max_retry:
        try: 
            if response.status_code == 200:
                return response.json()
            # return url if API limit reached
            # elif response.status_code == 429:
            #     time.sleep(0.2)
        except requests.RequestException as e:
            print(f'Error fetching data: {e.status_code} with URL {url}')
        
        retries = retries + 1


def filter_stations(stations:list[dict]) -> list[dict]:
    """Filter stations:
        1. fileter out stations without free bikes
        2. add active to station
        3. add free_ratio to station
        4. add coordinates to station
        5. delete internal_id from station
    """
    stations_vith_bikes = []
    for station in stations:
        # delete station without free bikes
        if station.get('free_bikes') == 0:
            continue
        # add 'active' and delete 'status'
        station['active'] = True if station.pop('status') == 'aktiv' else False
        # add 'free ratio'
        station['free_ratio'] = station['free_boxes'] / station['boxes']
        # add 'coordinates' as [longitude, latitude]
        station['coordinates'] = [station.pop('longitude'), station.pop('latitude')]
        # delete 'internal_id'
        del station['internal_id']

        stations_vith_bikes.append(station)
    
    return stations_vith_bikes


def get_address_url(station: dict) -> str:
    """
    Return URL for station address fetching.
    e.g. https://api.i-mobility.at/routing/api/v1/nearby_address?latitude=48.191&longitude=16.330 
    """
    coordinates = station.get('coordinates')
    return f'https://api.i-mobility.at/routing/api/v1/nearby_address?latitude={round(coordinates[1],3)}&longitude={round(coordinates[0],3)}'


def fetch_station_addresses(urls: list) -> list:
    """Fetch station addresses."""

    # with ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     addresses = list(executor.map(fetch_data, urls))
    address = [] 
    for url in urls:
        address.append(fetch_data(url))

    return address


def build_adresses_dict(addresses: list[dict])-> dict:
    """
    Build distionary for address names. 
    {(longitude, latitude): name}
    """

    address_dict = {}
    for address in addresses:
        data = address.get('data')
        coordinates = data.get('coordinate')
        address_dict[(round(coordinates.get('longitude'),3), round(coordinates.get('latitude'),3))] = data.get('name')

    return address_dict


def add_addresses_to_stations(stations: list[dict], address_dict: dict) -> list[dict]:
    """
    Add address to stations if coordinates match.
    """
    for station in stations:
        coordinate = station.get('coordinates')
        station['address'] = address_dict.get(tuple([round(coordinate[0],3), round(coordinate[1],3)]), 'Does not match coordinates')

    return stations


if __name__ == '__main__':

    if stations := fetch_data(STATIONS_URL):
        # filter stations and add 'active', 'free_ratio', 'coordinates' and delete 'status' and 'internal_id'
        stations = filter_stations(stations)
        # Sort stations by the number of free bikes descending. If two stations have the same number
        # of bikes, sort by name ascending.
        stations = sorted(stations, key=lambda x: (-x['free_bikes'], x['name']))
        
        # STEP 2:
        # Build URLs for addresses requests
        addresses_urls = [get_address_url(station) for station in stations]
        # fetch data for addresses
        addresses = fetch_station_addresses(addresses_urls)
        # build addresses dict 
        addresses_dict = build_adresses_dict(addresses)
        # add adress to station
        stations = add_addresses_to_stations(stations, addresses_dict)

        for station in stations:
            print(station)

