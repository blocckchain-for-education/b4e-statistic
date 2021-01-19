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
# ------------------------------------------------------------------------------
import datetime
from json.decoder import JSONDecodeError
import logging
import time

from aiohttp.web import json_response
import bcrypt
from Crypto.Cipher import AES
from itsdangerous import BadSignature
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer

from rest_api.b4e_rest_api.errors import ApiBadRequest
from rest_api.b4e_rest_api.errors import ApiNotFound
from rest_api.b4e_rest_api.errors import ApiUnauthorized

from rest_api.b4e_rest_api.blockchain_get_data import get_data_from_transaction
from rest_api.b4e_rest_api.blockchain_get_data import get_state
from rest_api.b4e_rest_api.blockchain_get_data import get_student_data
from rest_api.b4e_rest_api.blockchain_get_data import get_record_transaction
from config.config import Sawtooth_Config

LOGGER = logging.getLogger(__name__)


def slice_per(source, step):
    if len(source) < step:
        return [source]
    return [source[i::step] for i in range(step)]


def tolist(source):
    list_temp = []
    for list_ in source:
        for element in list_:
            list_temp.append(element)
    return list_temp


class RouteHandler(object):
    def __init__(self, loop, messenger, database):
        self._loop = loop
        self._messenger = messenger
        self._database = database

    async def get_new_key_pair(self, request):
        public_key, private_key = self._messenger.get_new_key_pair()
        return json_response(
            {
                'publicKey': public_key,
                'privateKey': private_key
            })

    async def get_num_cert_each_season(self,request):
        statistic = self._database.num_cert_each_season()
        return json_response({"data": statistic})

    async def get_num_subject_each_season(self,request):
        statistic = self._database.num_subject_each_season()
        return json_response({"data": statistic})

    async def get_num_active_institutions(self,request):
        statistic = self._database.num_active_institutions()
        return json_response({"data": statistic})



async def decode_request(request):
    try:
        return await request.json()
    except JSONDecodeError:
        raise ApiBadRequest('Improper JSON format')


def validate_fields(required_fields, body):
    for field in required_fields:
        if body.get(field) is None:
            raise ApiBadRequest(
                "'{}' parameter is required".format(field))


def encrypt_private_key(aes_key, public_key, private_key):
    init_vector = bytes.fromhex(public_key[:32])
    cipher = AES.new(bytes.fromhex(aes_key), AES.MODE_CBC, init_vector)
    return cipher.encrypt(private_key)


def decrypt_private_key(aes_key, public_key, encrypted_private_key):
    init_vector = bytes.fromhex(public_key[:32])
    cipher = AES.new(bytes.fromhex(aes_key), AES.MODE_CBC, init_vector)
    private_key = cipher.decrypt(bytes.fromhex(encrypted_private_key))
    return private_key


def hash_password(password):
    return bcrypt.hashpw(bytes(password, 'utf-8'), bcrypt.gensalt())


def get_time():
    dts = datetime.datetime.utcnow()
    return round(time.mktime(dts.timetuple()) + dts.microsecond / 1e6)


def generate_auth_token(secret_key, public_key):
    serializer = Serializer(secret_key)
    token = serializer.dumps({'public_key': public_key})
    return token.decode('ascii')


def deserialize_auth_token(secret_key, token):
    serializer = Serializer(secret_key)
    return serializer.loads(token)
