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
        if field == '_id' or field == 'GISJOIN':
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


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Invalid Usage: python3 updateMetadata.py <collection_name>')

    else:
        collection_name = sys.argv[1]
        main(collection_name)



'''

db.createCollection('testing_metadata_script')
db.testing_metadata_script.insert([{'test': 0, 'testStr': 'potato', 'epoch_time': 1656968989000}, {'test': 1, 'testSingle': 11, 'testStr': 'juno', 'epoch_time': 1656963989000}, {'test2': 17, 'testStr': 'daisyPants', 'epoch_time': 1156968989000}, {'test2': 7, 'testSingle': 11, 'testStr': 'daisyPants', 'epoch_time': 1156968989000}])
db.testing_metadata_script.find()

python3 updateMetadata.py testing_metadata_script

db.Metadata.find({'collection': 'testing_metadata_script'}).pretty()
db.testing_metadata_script.drop()

'''
