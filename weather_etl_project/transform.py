from datetime import datetime
import pandas as pd

def transform_weather_data(data):

    	location = data["location"]
    	current = data["current"]

    	transformed = {
        	"city": location["name"],
        	"region": location["region"],
        	"country": location["country"],
        	"temperature_c": current["temp_c"],
        	"humidity": current["humidity"],
        	"wind_kph": current["wind_kph"],
        	"condition": current["condition"]["text"],
        	"last_updated": current["last_updated"],
        	"extracted_at": datetime.now()
    	}

    	df = pd.DataFrame([transformed])

    	return df
