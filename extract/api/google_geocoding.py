"""
Most common tools for simple work with Google's Geocoding API. 
"""


from .api import simple_data

long_address_ref = {
    'street_number': 'street_number',
    'political': 'city',
    'locality': 'del_mun',
    'administrative_area_level_1': 'state',
    'postal_code': 'zip_code'
}

short_address_ref = {
    'country': 'country'
}

def get_place_data(address_components: dict) -> dict:
    """
    From Googles Geocoding API json formated response,
    builds a Google Place data dict based on observed trials.

    Just pass the data, here will be cleaned.
    
    This SHOULD be modified according to Developer's country/place needs
    """
    place = {}

    for component in address_components:
        comp_type = component['types'][0]
        
        # Not all results have the same structure, but this seems to be quite good for a
        # first approach
        
        if comp_type in {'route', 'establishment', 'transit_station'}:
            place['street_name'] = component['long_name']

        if comp_type in long_address_ref:
            place[long_address_ref[comp_type]] = component['long_name']

        if comp_type in short_address_ref:
            place[short_address_ref[comp_type]] = component['short_name']

    return place

def google_geocoding(lat: float, long: float, key: float) -> dict:
    """
    Using Latituted (lat) and Longitude (long) values, will return a
    Google Place object, that acts as a dictionary with the common values requested
    of a place.

    Remember that you are free to modify it. 
    """
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{long}&key={key}"

    data = simple_data(url = url)

    address_components = data['results'][0]['address_components']

    place = get_place_data(address_components)

    return place