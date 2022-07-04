import pymongo
import os
import urllib.parse

if __name__ == '__main__':
    username = urllib.parse.quote_plus(os.environ.get('ROOT_MONGO_USER'))
    password = urllib.parse.quote_plus(os.environ.get('ROOT_MONGO_PASS'))
    mongo = pymongo.MongoClient(f'mongodb://{username}:{password}@lattice-100:27018/')
    db = mongo['sustaindb']
    population = db['county_total_population']
    metadata = db['Metadata']

    min = int(population.find({"2020_total_population": {'$ne' : None}}).sort('2020_total_population', 1).limit(1).next()['2020_total_population'])
    max = int(population.find({"2020_total_population": {'$ne' : None}}).sort('2020_total_population', -1).limit(1).next()['2020_total_population'])

    metadata.update_one(
        {'collection': 'county_total_population'},
        {'$set': 
            {'fieldMetadata':
                [
                    {
                        "name" : "STATEFP",
                        "type" : "NUMBER",
                        "min" : 1,
                        "max" : 72
                    },
                    {
                        "name" : "2010_total_population",
                        "type" : "NUMBER",
                        "min" : 82,
                        "max" : 9818605
                    },
                    {
                        "name" : "2020_total_population",
                        "type" : "NUMBER",
                        "min" : min,
                        "max" : max
                    },
                    {
                        "name" : "COUNTYFP",
                        "type" : "NUMBER",
                        "min" : 1,
                        "max" : 840
                    },
                    {
                        "name" : "COUNTYNH",
                        "type" : "NUMBER",
                        "min" : 10,
                        "max" : 8400
                    },
                    {
                        "name" : "STATE",
                        "type" : "STRING",
                        "values" : [
                            "Iowa",
                            "Ohio",
                            "Maryland",
                            "Pennsylvania",
                            "Kentucky",
                            "Delaware",
                            "Louisiana",
                            "Illinois",
                            "Massachusetts",
                            "North Carolina",
                            "Minnesota",
                            "Oregon",
                            "Rhode Island",
                            "New Mexico",
                            "South Dakota",
                            "Kansas",
                            "Tennessee",
                            "Vermont",
                            "Wisconsin",
                            "Nevada",
                            "Texas",
                            "Oklahoma",
                            "Idaho",
                            "Arkansas",
                            "Nebraska",
                            "Virginia",
                            "Wyoming",
                            "Puerto Rico",
                            "District Of Columbia",
                            "New York",
                            "Alabama",
                            "Maine",
                            "Missouri",
                            "New Hampshire",
                            "South Carolina",
                            "Washington",
                            "Hawaii",
                            "Michigan",
                            "Arizona",
                            "Florida",
                            "Mississippi",
                            "Montana",
                            "Colorado",
                            "Utah",
                            "North Dakota",
                            "West Virginia",
                            "Indiana",
                            "Alaska",
                            "Georgia",
                            "New Jersey",
                            "California",
                            "Connecticut"
                        ]
                    },
                    {
                        "name" : "STATENH",
                        "type" : "NUMBER",
                        "min" : 10,
                        "max" : 720
                    }
                ]
            }
        }
    )