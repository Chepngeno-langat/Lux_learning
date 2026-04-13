import requests
from config import API_KEY, CITY

def extract_weather_data():
	url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}"
	
	response = requests.get(url)
	if response.status_code != 200:
		raise Exception("API request failed!")
	
	return response.json()

