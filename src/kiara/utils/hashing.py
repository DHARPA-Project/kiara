# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import dag_cbor
import hashlib
from dag_cbor.encoding import EncodableType
from multiformats import CID, multihash
from multiformats.multicodec import Multicodec
from multiformats.multihash import Multihash
from multiformats.varint import BytesLike
from typing import Tuple, Union


def compute_cid(
    data: EncodableType,
    hash_codec: str = "sha2-256",
    encode: str = "base58btc",
) -> Tuple[bytes, CID]:

    encoded = dag_cbor.encode(data)
    hash_func = multihash.get(hash_codec)
    digest = hash_func.digest(encoded)

    cid = CID(encode, 1, codec="dag-cbor", digest=digest)
    return encoded, cid


_, NONE_CID = compute_cid(data=None)


def compute_cid_from_file(
    file: str, codec: Union[str, int, Multicodec] = "raw", hash_codec: str = "sha2-256"
):

    assert hash_codec == "sha2-256"

    hash_func = hashlib.sha256
    file_hash = hash_func()

    CHUNK_SIZE = 65536
    with open(file, "rb") as f:
        fb = f.read(CHUNK_SIZE)
        while len(fb) > 0:
            file_hash.update(fb)
            fb = f.read(CHUNK_SIZE)

    wrapped = multihash.wrap(file_hash.digest(), "sha2-256")
    return create_cid_digest(digest=wrapped, codec=codec)


def create_cid_digest(
    digest: Union[
        str, BytesLike, Tuple[Union[str, int, Multihash], Union[str, BytesLike]]
    ],
    codec: Union[str, int, Multicodec] = "raw",
) -> CID:

    cid = CID("base58btc", 1, codec, digest)
    return cid
