import pymongo
import os
import urllib.parse
import sys
import json

output_file = open('metadata.json', 'w')

def main(collection_name):
    username = urllib.parse.quote_plus(os.environ.get('ROOT_MONGO_USER'))
    password = urllib.parse.quote_plus(os.environ.get('ROOT_MONGO_PASS'))
    mongo = pymongo.MongoClient(f'mongodb://{username}:{password}@lattice-100:27018/')
    db = mongo['sustaindb']
    metadata = db['Metadata']
    collection = db[collection_name]

    try:
        collection.find().limit(1).next()
    except StopIteration:
        print('Invalid Collection Name')
        sys.exit(1)

    field_metadata = build_field_metadata(collection)
    update_metadata_collection(collection_name, field_metadata, metadata)
    print('Complete')

    build_aperture_metadata(field_metadata)

    output_file.close()


def update_metadata_collection(collection_name, field_metadata, metadata):
    with open('metadata.json', 'w') as f:
        f.write(json.dumps(field_metadata, indent=4))
    if metadata.count_documents({'collection': collection_name}) > 0:
        metadata.update_one(
            {'collection': collection_name},
            {'$set': {'fieldMetadata': field_metadata}}
        )
    else:
        document = {
            'collection': collection_name,
            'fieldMetadata': field_metadata
        }
        metadata.insert_one(document)


def build_field_metadata(collection):

    all_keys = collection.aggregate([ {"$project":{"arrayofkeyvalue":{"$objectToArray":"$$ROOT"}}}, {"$unwind":"$arrayofkeyvalue"}, {"$group":{"_id":None,"allkeys":{"$addToSet":"$arrayofkeyvalue.k"}}} ]).next()['allkeys']
    field_metadata = []
    index = 0

    for field in all_keys:
        # This is a hard-coded list of values we KNOW we want to exclude from metadata, for all or for specific collections
        if field == '_id' or field == 'GISJOIN' or field == 'MonitoringLocationIdentifier' or field == 'GridCode':
            index += 1
        else:
            try:
                document = collection.find({field: {'$exists': True}}).limit(1).next()
                field_type = type(document[field])
                if field == 'epoch_time':
                    (min, max) = find_min_max(collection, field)
                    metadata_entry = {
                        'name': field,
                        'type': 'DATE',
                        'minDate': min,
                        'maxDate': max
                    }
                    field_metadata.append(metadata_entry)
                elif field_type == str:
                    values = collection.distinct(field)
                    metadata_entry = {
                        'name': field,
                        'type': 'STRING',
                        'values': values
                    }
                    field_metadata.append(metadata_entry)
                elif field_type == int or field_type == float:
                    (min, max) = find_min_max(collection, field)
                    if type(min) != bool:
                        metadata_entry = {
                            'name': field,
                            'type': 'NUMBER',
                            'min': min,
                            'max': max
                        }
                    else :
                        metadata_entry = {
                            'name': field,
                            'type': 'NUMBER',
                            'max': max
                        }
                    field_metadata.append(metadata_entry)
            except StopIteration:
                print(f'Error processing {field}, moving on...')

            index += 1
            percent_done = round(index / len(all_keys) * 100, 3)
            print(f'{percent_done}% Done - Finished Processing {field}')

    return field_metadata


def find_min_max(collection, field):
    min = collection.find({field: {'$ne' : None}}).sort(field, 1).limit(1).next()[field]
    max = collection.find({field: {'$ne' : None}}).sort(field, -1).limit(1).next()[field]
    if min == max:
        min = False
    return min, max


'''
NOTE this will only generate basic menumetadata. You may need to update this output file
if some of your entries are temporal, need labels, should be hidden by default, etc...
'''
def build_aperture_metadata(field_metadata):
    aperture_field_metadata = []
    for index, datum in enumerate(field_metadata):
        hide_by_default = False if index <= 5 else True
        if datum['name'] == 'epoch_time':
            entry = {
                'name': 'epoch_time',
                'label': 'Date',
                'type': 'date',
                'step': 'day'
            }
        else:
            entry = {
                'name': datum['name'],
                'hideByDefault': hide_by_default
            }
        aperture_field_metadata.append(entry)
    with open('aperture_menumetadata.json', 'w') as f:
        f.write(json.dumps(aperture_field_metadata, indent=4))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Invalid Usage: python3 updateMetadata.py <collection_name>')

    else:
        collection_name = sys.argv[1]
        main(collection_name)

