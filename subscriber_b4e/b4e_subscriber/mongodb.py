# Copyright 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

import logging
from pymongo import MongoClient

from config.config import MongoDBConfig

LOGGER = logging.getLogger(__name__)


class Database(object):
    """Simple object for managing a connection to a postgres database
    """

    def __init__(self):
        self.mongo = None
        self.b4e_db = None
        self.b4e_actor_collection = None
        self.b4e_record_collection = None
        self.b4e_record_manager_collection = None
        self.b4e_voting_collection = None
        self.b4e_vote_collection = None
        self.b4e_environment_collection = None
        self.b4e_class_collection = None
        self.b4e_block_collection = None

    def connect(self, host=MongoDBConfig.HOST, port=MongoDBConfig.PORT, user_name=MongoDBConfig.USER_NAME,
                password=MongoDBConfig.PASSWORD):
        if (user_name != "" and password != ""):
            url = f"mongodb://{user_name}:{password}@{host}:{port}"
            self.mongo = MongoClient(url)
        else:
            self.mongo = MongoClient(host=host, port=int(port))
        self.create_collections()

    def create_collections(self):
        self.b4e_db = self.mongo[MongoDBConfig.DATABASE]
        self.b4e_actor_collection = self.b4e_db[MongoDBConfig.ACTOR_COLLECTION]
        self.b4e_record_collection = self.b4e_db[MongoDBConfig.RECORD_COLLECTION]
        self.b4e_record_manager_collection = self.b4e_db[MongoDBConfig.RECORD_MANAGER_COLLECTION]
        self.b4e_voting_collection = self.b4e_db[MongoDBConfig.VOTING_COLLECTION]
        self.b4e_vote_collection = self.b4e_db[MongoDBConfig.VOTE_COLLECTION]
        self.b4e_environment_collection = self.b4e_db[MongoDBConfig.ENVIRONMENT_COLLECTION]
        self.b4e_class_collection = self.b4e_db[MongoDBConfig.CLASS_COLLECTION]
        self.b4e_block_collection = self.b4e_db[MongoDBConfig.BLOCK_COLLECTION]

    def disconnect(self):
        self.mongo.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def drop_fork(self, block_num):
        """Deletes all resources from a particular block_num
                """
        delete = {"block_num": {"$gte": block_num}}

        try:
            self.b4e_record_collection.delete_many(delete)
            self.b4e_actor_collection.delete_many(delete)
            self.b4e_record_collection.delete_many(delete)
            self.b4e_voting_collection.delete_many(delete)
            self.b4e_environment_collection.delete_many(delete)
            self.b4e_block_collection.delete_many(delete)

        except Exception as e:
            print(e)

    def fetch_last_known_blocks(self, count):
        try:
            blocks = list(self.b4e_block_collection.find().sort("block_num", -1))
            return blocks[:count]
            # if not found res will not contain ['hits']['hits'][0]['_source']
        except IndexError:
            print("not found block")
            return None

    def fetch_block(self, block_num):
        if not block_num:
            return None

        query = {"block_num": block_num}
        try:
            block = self.b4e_block_collection.find_one(query)
            return block
        except Exception as e:
            print(e)
            return None

    def insert_block(self, block_dict):
        try:

            key = {'block_num': block_dict['block_num']}
            data = {"$set": block_dict}

            res = self.b4e_block_collection.update_one(key, data, upsert=True)
            return res
        except Exception as e:
            print(e)
            return None

    def insert_actor(self, actor_dict):
        try:
            key = {'actor_public_key': actor_dict['actor_public_key']}
            data = {"$set": actor_dict}
            actor_dict['info'][-1]['block_num'] = actor_dict['block_num']
            old_actor = self.b4e_actor_collection.find_one(key)
            if old_actor:
                for i in range(len(old_actor['info'])):
                    actor_dict['info'][i] = old_actor['info'][i]
                actor_dict['end_block_num'] = actor_dict['block_num']
                actor_dict['block_num'] = old_actor.get('block_num')

            res = self.b4e_actor_collection.update_one(key, data, upsert=True)
            return res
        except Exception as e:
            print(e)
            return None

    def insert_record(self, record_dict):
        try:
            key = {'record_id': record_dict['record_id']}
            data = {"$set": record_dict}
            record_dict['record_data'][-1]['block_num'] = record_dict['block_num']
            old_record = self.b4e_record_collection.find_one(key)
            if old_record:
                for i in range(len(old_record['record_data'])):
                    record_dict['record_data'][i] = old_record['record_data'][i]
                record_dict['end_block_num'] = record_dict['block_num']
                record_dict['block_num'] = old_record.get('block_num')
            res = self.b4e_record_collection.update_one(key, data, upsert=True)

            key = {'manager_public_key': record_dict['manager_public_key']}
            push = {'$push': {record_dict['record_type']: record_dict}}

            self.b4e_record_manager_collection.update(key, push, upsert=True)
            return res

        except Exception as e:
            print(e)
            return None

    def insert_voting(self, voting_dict):
        try:

            key = {'elector_public_key': voting_dict['elector_public_key']}
            try:
                voting_dict['vote'][-1]['block_num'] = voting_dict['block_num']
                voting_dict['vote'][-1]['elector_public_key'] = voting_dict['elector_public_key']
                self.insert_vote(voting_dict['vote'][-1])
                old_voting = self.b4e_voting_collection.find_one(key)
                if old_voting:
                    for i in range(len(old_voting)):
                        voting_dict['vote'][i] = old_voting['vote'][i]
                    voting_dict['end_block_num'] = voting_dict['block_num']
                    voting_dict['block_num'] = old_voting.get('block_num')
            except Exception as e:
                LOGGER.info("Create Voting")

            data = {"$set": voting_dict}
            res = self.b4e_voting_collection.update_one(key, data, upsert=True)
            return res
        except Exception as e:
            print(e)
            return None

    def insert_vote(self, vote_dict):
        try:
            key = {'issuer_public_key': vote_dict['issuer_public_key'],
                   'elector_public_key': vote_dict['elector_public_key']}
            data = {"$set": vote_dict}
            old_vote = self.b4e_vote_collection.find_one(key)
            if old_vote:
                return
            res = self.b4e_vote_collection.update_one(key, data, upsert=True)
            return res
        except Exception as e:
            print(e)
            return None

    def insert_environment(self, environment_dict):
        try:
            key = {}
            data = {"$set": environment_dict}
            old_environment = self.b4e_record_collection.find_one(key)
            if old_environment:
                environment_dict['end_block_num'] = environment_dict['block_num']
                environment_dict['block_num'] = old_environment.get('block_num')
            res = self.b4e_environment_collection.update_one(key, data, upsert=True)
            return res
        except Exception as e:
            print(e)
            return None

    def insert_class(self, class_dict):
        try:

            key = {
                'teacher_public_key': class_dict['teacher_public_key'],
                'class_id': class_dict['class_id']
            }

            data = {"$set": class_dict}
            old_class = self.b4e_class_collection.find_one(key)
            if old_class:
                class_dict['end_block_num'] = class_dict['block_num']
                class_dict['block_num'] = old_class.get('block_num')
            res = self.b4e_class_collection.update_one(key, dict(data), upsert=True)
            return res
        except Exception as e:
            print(e)
            LOGGER.warning(e)
            return None
